package server

import (
	"html/template"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

// Ragnarok Online chat item links arrive on the wire as a tag the client
// renders into a clickable, colored item name:
//
//	<ITEML>000481zD%04&0h'00)15Q)00)00)00</ITEML>
//
// The inner blob is the client's own packed encoding of the linked item
// (item id + refine + four card slots + random options), base62-encoded. We
// decode it here so the public chat log shows e.g. "[+4 Hat (Drooping Kitty
// Card)]" instead of the raw garbage. The codec matches OpenKore's
// solveItemLink (github.com/OpenKore/openkore, issue #2477 / PR #2541).
//
// Layout of the inner blob:
//
//	HEAD          leading run of [0-9A-Za-z]: 5-char link id, 1 "show slots"
//	              flag, then the item id (base62). Item id = base62(HEAD[6:]).
//	%<base62>     refine level
//	&<base62>     equip location (ignored for display)
//	(<base62> or  card slot (up to four; 0 = empty). Different client builds
//	)<base62>     use '(' or ')' as the card separator, so we accept both.
//	*...          random options (ignored for display)
//
// Anything we cannot confidently decode is replaced with a neutral "🔗[item]"
// marker rather than left as garbage.

// base62 alphabet used by the item-link codec: digits, then lower, then upper.
const itemLinkBase62Alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

var itemLinkBase62Val = func() map[byte]int64 {
	m := make(map[byte]int64, len(itemLinkBase62Alphabet))
	for i := 0; i < len(itemLinkBase62Alphabet); i++ {
		m[itemLinkBase62Alphabet[i]] = int64(i)
	}
	return m
}()

// itemLinkTagRe matches a single <ITEML>...</ITEML> (or legacy <ITEM>...</ITEM>)
// tag. The inner blob never contains '<', so [^<]* is a safe, non-greedy body.
var itemLinkTagRe = regexp.MustCompile(`<ITEM[A-Z]?>([^<]*)</ITEM[A-Z]?>`)

// itemLinkFieldRe pulls the leading HEAD run plus every "<sep><base62>" field
// out of an inner blob.
var (
	itemLinkHeadRe  = regexp.MustCompile(`^([0-9A-Za-z]+)`)
	itemLinkFieldRe = regexp.MustCompile(`([%&'()*])([0-9A-Za-z]+)`)
)

// base62Decode converts an item-link base62 string to its integer value.
func base62Decode(s string) (int64, bool) {
	if s == "" {
		return 0, false
	}
	var n int64
	for i := 0; i < len(s); i++ {
		v, ok := itemLinkBase62Val[s[i]]
		if !ok {
			return 0, false
		}
		n = n*62 + v
	}
	return n, true
}

// decodedItemLink is the result of parsing one item-link blob.
type decodedItemLink struct {
	itemID int64
	refine int64
	cards  []int64 // non-zero card item ids, in slot order
	ok     bool
}

// decodeItemLink parses the inner blob of an <ITEML> tag.
func decodeItemLink(inner string) decodedItemLink {
	var d decodedItemLink

	head := itemLinkHeadRe.FindString(inner)
	// Need 5-char link id + 1 flag + at least one item-id char.
	if len(head) < 7 {
		return d
	}
	itemID, ok := base62Decode(head[6:])
	if !ok || itemID <= 0 {
		return d
	}
	d.itemID = itemID
	d.ok = true

	for _, m := range itemLinkFieldRe.FindAllStringSubmatch(inner, -1) {
		sep, val := m[1], m[2]
		switch sep {
		case "%":
			if r, ok := base62Decode(val); ok {
				d.refine = r
			}
		case "(", ")":
			if c, ok := base62Decode(val); ok && c > 0 {
				d.cards = append(d.cards, c)
			}
		}
	}
	return d
}

// itemNameByID resolves an item id to its display name for the given language,
// using the in-memory item cache. Returns ("", false) if the item is unknown.
func itemNameByID(id int64, lang string) (string, bool) {
	ensureItemCache()
	itemCacheMu.RLock()
	defer itemCacheMu.RUnlock()
	it, ok := itemByIDCache[id]
	if !ok {
		return "", false
	}
	if lang == "pt" && it.namePT != "" {
		return it.namePT, true
	}
	if it.name != "" {
		return it.name, true
	}
	return it.namePT, it.namePT != ""
}

// itemByAegis resolves an aegis name (the rAthena-style "Yoyo_Tail" identifier
// used in the YAML mob seed) to its item id and localized display name. Returns
// ("", 0, false) when no item with that aegis name is known.
func itemByAegis(aegis, lang string) (string, int64, bool) {
	ensureItemCache()
	itemCacheMu.RLock()
	id, ok := itemByAegisCache[strings.ToLower(aegis)]
	itemCacheMu.RUnlock()
	if !ok {
		return "", 0, false
	}
	if name, ok := itemNameByID(id, lang); ok {
		return name, id, true
	}
	return "", 0, false
}

// renderItemLink turns one decoded blob into an HTML fragment. The linked item
// name (when known) links to its market history page; the refine prefix and any
// slotted cards are appended as plain text.
func renderItemLink(inner, lang string) string {
	d := decodeItemLink(inner)
	if !d.ok {
		return `<span class="text-sky-600 dark:text-sky-400">🔗[item]</span>`
	}

	name, known := itemNameByID(d.itemID, lang)

	var label strings.Builder
	if d.refine > 0 {
		label.WriteString("+")
		label.WriteString(strconv.FormatInt(d.refine, 10))
		label.WriteString(" ")
	}
	if known {
		label.WriteString(name)
	} else {
		label.WriteString("item #")
		label.WriteString(strconv.FormatInt(d.itemID, 10))
	}
	if len(d.cards) > 0 {
		cardNames := make([]string, 0, len(d.cards))
		for _, c := range d.cards {
			if cn, ok := itemNameByID(c, lang); ok {
				cardNames = append(cardNames, cn)
			} else {
				cardNames = append(cardNames, "card #"+strconv.FormatInt(c, 10))
			}
		}
		label.WriteString(" (")
		label.WriteString(strings.Join(cardNames, ", "))
		label.WriteString(")")
	}

	text := template.HTMLEscapeString("[" + label.String() + "]")
	if !known {
		return `<span class="text-sky-600 dark:text-sky-400">` + text + `</span>`
	}
	href := "/item?name=" + url.QueryEscape(name)
	return `<a href="` + template.HTMLEscapeString(href) + `" class="text-sky-600 dark:text-sky-400 hover:underline">` + text + `</a>`
}

// renderChatMessage replaces every item-link tag in a chat message with its
// decoded, linkable name and HTML-escapes the surrounding text, returning markup
// safe to emit directly. Messages with no item links are simply escaped.
func renderChatMessage(msg, lang string) template.HTML {
	if !strings.Contains(msg, "<ITEM") {
		return template.HTML(template.HTMLEscapeString(msg))
	}

	var b strings.Builder
	last := 0
	for _, m := range itemLinkTagRe.FindAllStringSubmatchIndex(msg, -1) {
		b.WriteString(template.HTMLEscapeString(msg[last:m[0]]))
		b.WriteString(renderItemLink(msg[m[2]:m[3]], lang))
		last = m[1]
	}
	b.WriteString(template.HTMLEscapeString(msg[last:]))
	return template.HTML(b.String())
}
