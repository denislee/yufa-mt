package server

import (
	"testing"
	"time"
)

// capturedMobInfo1001 is the exact decoded line sequence from a live
// `@mobinfo 1001` reply on the server (Scorpion), as seen in the pcap.
var capturedMobInfo1001 = []string{
	"Monstro: 'Escorpião'/'Escorpião'/'SCORPION' (1001)",
	"Nv:24  HP:1109  EXP Base:287  EXP Classe:176  HIT:76  ESQV:48",
	"DEF:30  DEFM:0  FOR:1  AGI:24  VIT:24  INT:5  DES:52  SOR:5",
	"ATQ:80~135  Alcance:1~10~12  Tamanho:Small  Raça: Insect  Elemento: Fire (Nv:1)",
	"Drops:",
	"  - <ITEML>000000fY'00)00)00)00)00</ITEML>  0.70% - <ITEML>000000eA'00)00)00)00)00</ITEML>  55.00% - <ITEML>000000cd'00)00)00)00)00</ITEML>  0.57%",
	"  - <ITEML>000000fd'00)00)00)00)00</ITEML>  2.10% - <ITEML>0000001Pz'00)00)00)00)00</ITEML>  1.00% - <ITEML>0000008c'00)00)00)00)00</ITEML>  2.00%",
	"  - <ITEML>000000a5'00)00)00)00)00</ITEML>  0.20% - <ITEML>00002013C'00)00)00)00)00</ITEML>  0.01%",
}

// feed runs lines through the accumulator without touching the DB, capturing
// the finalized block instead of upserting it.
func feed(t *testing.T, lines []string) *mobBlock {
	t.Helper()
	var a mobInfoAccumulator
	var got *mobBlock
	finalize := func() {
		if a.cur != nil && a.cur.id != 0 {
			got = a.cur
		}
		a.cur = nil
	}
	now := time.Now()
	for _, ln := range lines {
		if m := mobInfoHeaderRe.FindStringSubmatch(ln); m != nil {
			finalize()
			id := atoiSafe(m[5])
			a.cur = &mobBlock{id: id, name: m[2], aegisName: m[4], isMvp: m[1] != ""}
			continue
		}
		if a.cur != nil && isMobInfoContinuation(ln) {
			a.cur.apply(ln)
		}
	}
	finalize()
	_ = now
	return got
}

func TestMobInfoParseScorpion(t *testing.T) {
	b := feed(t, capturedMobInfo1001)
	if b == nil {
		t.Fatal("no block finalized")
	}
	if b.id != 1001 {
		t.Errorf("id = %d, want 1001", b.id)
	}
	if b.name != "Escorpião" {
		t.Errorf("name = %q, want Escorpião", b.name)
	}
	if b.aegisName != "SCORPION" {
		t.Errorf("aegisName = %q, want SCORPION", b.aegisName)
	}
	if !b.level.Valid || b.level.Int64 != 24 {
		t.Errorf("level = %v, want 24", b.level)
	}
	if !b.hp.Valid || b.hp.Int64 != 1109 {
		t.Errorf("hp = %v, want 1109", b.hp)
	}
	if !b.baseExp.Valid || b.baseExp.Int64 != 287 {
		t.Errorf("baseExp = %v, want 287", b.baseExp)
	}
	if !b.jobExp.Valid || b.jobExp.Int64 != 176 {
		t.Errorf("jobExp = %v, want 176", b.jobExp)
	}
	if !b.def.Valid || b.def.Int64 != 30 {
		t.Errorf("def = %v, want 30", b.def)
	}
	if !b.mdef.Valid || b.mdef.Int64 != 0 {
		t.Errorf("mdef = %v, want 0", b.mdef)
	}
	// FOR/AGI/VIT/INT/DES/SOR -> str/agi/vit/int/dex/luk
	if !b.str.Valid || b.str.Int64 != 1 {
		t.Errorf("str = %v, want 1", b.str)
	}
	if !b.dex.Valid || b.dex.Int64 != 52 {
		t.Errorf("dex = %v, want 52", b.dex)
	}
	if !b.luk.Valid || b.luk.Int64 != 5 {
		t.Errorf("luk = %v, want 5", b.luk)
	}
	if !b.atkMin.Valid || b.atkMin.Int64 != 80 || !b.atkMax.Valid || b.atkMax.Int64 != 135 {
		t.Errorf("atk = %v~%v, want 80~135", b.atkMin, b.atkMax)
	}
	if !b.atkRange.Valid || b.atkRange.Int64 != 1 ||
		!b.skillRange.Valid || b.skillRange.Int64 != 10 ||
		!b.chaseRange.Valid || b.chaseRange.Int64 != 12 {
		t.Errorf("range = %v~%v~%v, want 1~10~12", b.atkRange, b.skillRange, b.chaseRange)
	}
	if b.size != "Small" {
		t.Errorf("size = %q, want Small", b.size)
	}
	if b.race != "Insect" {
		t.Errorf("race = %q, want Insect", b.race)
	}
	if b.element != "Fire" {
		t.Errorf("element = %q, want Fire", b.element)
	}
	if !b.elementLevel.Valid || b.elementLevel.Int64 != 1 {
		t.Errorf("elementLevel = %v, want 1", b.elementLevel)
	}
	if len(b.drops) != 8 {
		t.Fatalf("drops = %d, want 8: %+v", len(b.drops), b.drops)
	}
	// First drop: <ITEML>000000fY...> at 0.70%. Decodes via the existing codec.
	if b.drops[1].Rate != 55.0 {
		t.Errorf("second drop rate = %v, want 55.0", b.drops[1].Rate)
	}
	for i, d := range b.drops {
		if d.ItemID <= 0 {
			t.Errorf("drop[%d] has non-positive item id %d", i, d.ItemID)
		}
	}
	t.Logf("parsed drops: %+v", b.drops)
}

// capturedMobInfo1039 is the exact decoded line sequence from a live
// `@mobinfo 1039` reply on the server (Baphomet, an MVP), from the pcap.
var capturedMobInfo1039 = []string{
	"Monstro MVP: 'Baphomet'/'Bafomé'/'BAPHOMET' (1039)",
	"Nv:81  HP:668000  EXP Base:107250  EXP Classe:37895  HIT:201  ESQV:233",
	"DEF:35  DEFM:45  FOR:1  AGI:152  VIT:96  INT:85  DES:120  SOR:95",
	"ATQ:3220~4040  Alcance:2~10~12  Tamanho:Large  Raça: Demon  Elemento: Dark (Nv:3)",
	"Drops:",
	"  - <ITEML>000000eT'00)00)00)00)00</ITEML>  38.00% - <ITEML>0000y1nE&00'00)00)00)00)00</ITEML> [0]  2.00% - <ITEML>000481Ao&0F'00)00)00)00)00</ITEML> [0]  2.00%",
	"  - <ITEML>0002c1G3&00'00)00)00)00</ITEML>  8.00% - <ITEML>000000bw'00)00)00)00)00</ITEML>  5.00% - <ITEML>0000009X'00)00)00)00)00</ITEML>  30.00%",
	"  - <ITEML>000000fS'00)00)00)00)00</ITEML>  43.00% - <ITEML>000000fT'00)00)00)00)00</ITEML>  56.00% - <ITEML>00002014T'00)00)00)00</ITEML>  0.01%",
	"EXP Bônus MVP:53625",
	"Itens MVP: <ITEML>0000009N'00)00)00)00)00</ITEML>  20.00% - <ITEML>000000c6'00)00)00)00)00</ITEML>  4.00% - <ITEML>000000eT'00)00)00)00)00</ITEML>  38.00%",
}

func TestMobInfoParseBaphometMVP(t *testing.T) {
	b := feed(t, capturedMobInfo1039)
	if b == nil {
		t.Fatal("no block finalized")
	}
	if b.id != 1039 || b.name != "Baphomet" || b.aegisName != "BAPHOMET" {
		t.Errorf("header = id %d name %q aegis %q, want 1039/Baphomet/BAPHOMET", b.id, b.name, b.aegisName)
	}
	if !b.isMvp {
		t.Error("isMvp = false, want true for an MVP header")
	}
	if !b.level.Valid || b.level.Int64 != 81 {
		t.Errorf("level = %v, want 81", b.level)
	}
	if !b.hp.Valid || b.hp.Int64 != 668000 {
		t.Errorf("hp = %v, want 668000", b.hp)
	}
	if !b.atkMin.Valid || b.atkMin.Int64 != 3220 || !b.atkMax.Valid || b.atkMax.Int64 != 4040 {
		t.Errorf("atk = %v~%v, want 3220~4040", b.atkMin, b.atkMax)
	}
	if b.size != "Large" || b.race != "Demon" || b.element != "Dark" {
		t.Errorf("size/race/element = %q/%q/%q, want Large/Demon/Dark", b.size, b.race, b.element)
	}
	if !b.elementLevel.Valid || b.elementLevel.Int64 != 3 {
		t.Errorf("elementLevel = %v, want 3", b.elementLevel)
	}
	if !b.mvpExp.Valid || b.mvpExp.Int64 != 53625 {
		t.Errorf("mvpExp = %v, want 53625", b.mvpExp)
	}
	// 9 normal drops (incl. the two with "[0]" slot markers), 3 MVP items.
	if len(b.drops) != 9 {
		t.Errorf("normal drops = %d, want 9: %+v", len(b.drops), b.drops)
	}
	if len(b.mvpDrops) != 3 {
		t.Errorf("mvp drops = %d, want 3: %+v", len(b.mvpDrops), b.mvpDrops)
	}
	t.Logf("normal drops: %+v", b.drops)
	t.Logf("mvp drops: %+v", b.mvpDrops)
}

// A real chat line interleaved after a block must end it, not be swallowed.
func TestMobInfoContinuationIsStrict(t *testing.T) {
	if isMobInfoContinuation("golbin : bom dia!") {
		t.Error("ordinary chat must not be treated as a mobinfo continuation")
	}
	if !isMobInfoContinuation("Drops:") {
		t.Error("Drops: header should be a continuation")
	}
}
