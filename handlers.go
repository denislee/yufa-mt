package main

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"golang.org/x/crypto/bcrypt"
	"html/template"
	"log"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/agnivade/levenshtein" // <-- ADD THIS IMPORT
)

type BasePageData struct {
	Lang       string
	T          map[string]string
	RequestURL string
}

var translations = map[string]map[string]string{
	"en": {
		"market_summary":       "Market Summary",
		"search_by_item_name":  "Search by item name or ID...",
		"show_only_available":  "Show only available",
		"search":               "Search",
		"all_items":            "All Items",
		"showing_unique_items": "Showing <strong>%d</strong> unique items.",
		"item_name":            "Item Name",
		"item_id":              "Item ID",
		"available":            "Available",
		"lowest_price":         "Lowest Price",
		"highest_price":        "Highest Price",
		"updated_never":        "Updated: never",
		"updated_ago":          "Updated: %s ago",
		"nav_summary":          "Summary",
		"nav_full_list":        "Full List",
		"nav_activity":         "Activity",
		"nav_discord":          "Discord",
		"nav_chat":             "Chat",
		"nav_misc":             "Misc.",
		"nav_player_count":     "Player Count",
		"nav_xp_calculator":    "XP Calculator",
		"nav_about":            "About",
		"nav_rankings":         "Rankings",
		"nav_characters":       "Characters",
		"nav_guilds":           "Guilds",
		"nav_mvp_kills":        "MVP Kills",
		"nav_woe_rankings":     "WoE Rankings",
		// --- NEW for activity.html ---
		"recent_market_activity": "Recent Market Activity",
		"show_only_sold":         "Show only sold items",
		"filter":                 "Filter",
		"clear_filters":          "[Clear Filters]",
		"was_added":              "was added for sale.",
		"no_longer_for_sale":     "is no longer for sale.",
		"was_sold":               "was sold.",
		"vendor_logged_off":      "Vendor logged off or item was sold",
		"new_historical_low":     "New historical low",
		"for":                    "for",
		"no_market_activity":     "No market activity has been recorded yet.",
		"page_of":                "Page %d of %d",
		"previous":               "Previous",
		"next":                   "Next",

		// --- NEW for about.html ---
		"about_title":           "About This Application",
		"about_what_is_this":    "🏛️ What is This Site?",
		"about_welcome":         "Welcome! The goal of this site is to collect all public information from Project Yufa, whether on the official website, in the game, or on Discord, and organize it in one place, making it easy to search and consult.",
		"about_all_info_public": "All information here is publicly accessible. However, some of it is difficult to find or requires a lot of work to extract certain information. For this reason, I created this site, which is constantly being updated.",
		"about_warnings":        "⚠️ Important Notices",
		"about_please_read":     "Please read the following points carefully:",
		"about_warning_1":       "<strong>Do not</strong> consider the information on this site as the sole source of truth, as <strong>inconsistencies</strong> may exist here, since it collects information from different places at different times and on different days.",
		"about_warning_2":       "Some <strong>assumptions</strong> are data cross-references that the site makes, but which <strong>may also contain errors</strong> (<strong>for example</strong>: whether an item was sold or if the store closed, the AI's interpretation of Discord messages, the last time the character was active, etc.).",
		"about_warning_3":       "The information may be delayed. As the site takes a \"snapshot\" of various states from its sources at different intervals, there may be outdated information that <strong>does not</strong> reflect the present. Always check the indicator in the top right corner to see when the information was updated.",
		"about_warning_4":       "<strong>This</strong> site <strong>is</strong> completely independent and <strong>has no</strong> involvement from anyone in the Project Yufa <strong>administration</strong>. The only <strong>concession</strong> from the administrators was <strong>not</strong> to block this site's access to the official Project Yufa website. <strong>If</strong> they understand that this site harms the server, the community, or the <strong>operation</strong> of Project Yufa in any way, I will <strong>take</strong> the site down promptly.",
		"info_loaded_at":        "Information loaded: %s",

		// --- NEW for character_detail.html ---
		"last_updated_at":  "Last updated: %s",
		"char_info":        "Character Information",
		"rank":             "Rank",
		"base_level":       "Base Level",
		"job_level":        "Job Level",
		"experience":       "Experience",
		"zeny":             "Zeny",
		"status":           "Status",
		"active":           "Active",
		"inactive":         "Inactive",
		"last_active":      "Last Active",
		"mvp_kills_total":  "MVP Kills (%d Total)",
		"kills":            "kills",
		"no_mvp_kills":     "This player has no recorded MVP kills.",
		"char_changelog":   "Character Changelog",
		"timestamp":        "Timestamp",
		"activity":         "Activity",
		"first":            "First",
		"last":             "Last",
		"no_char_activity": "No recent activity recorded for this character.",
		"guild_info":       "Guild Information",
		"guild_name":       "Guild Name",
		"guild_level":      "Guild Level",
		"guild_master":     "Guild Master",
		"members":          "Members",
		"not_in_guild":     "This character is not in a guild.",
		"guild_history":    "Guild History",
		"no_guild_history": "No guild history recorded.",
		"guild_leader":     "Guild Leader",

		// --- NEW for character_changelog.html ---
		"char_changelog_title": "Character Changelog",
		"character":            "Character",
		"no_changelog_entries": "No changelog entries found.",
		"prev_short":           "Prev",
		"next_short":           "Next",
		// --- NEW for characters.html ---
		"characters_title":    "Characters",
		"search_by_name":      "Search by Name",
		"filter_by_class":     "Filter by Class",
		"all_classes":         "All Classes",
		"apply":               "Apply",
		"show_columns":        "Show Columns:",
		"filtering_by_guild":  "Filtering by Guild:",
		"clear_filter":        "[Clear Filter]",
		"stats_distribution":  "📊 Statistics & Distribution",
		"class_distribution":  "Class Distribution",
		"show_types":          "Show Types:",
		"novice":              "Novice",
		"first_class":         "First Class",
		"second_class":        "Second Class",
		"total_zeny_filtered": "Total Zeny (Filtered)",
		"sum_across_chars":    "Sum across %d characters (without bank).",
		"name":                "Name",
		"base_lvl":            "Base Lvl",
		"job_lvl":             "Job Lvl",
		"exp_perc":            "Exp %",
		"class":               "Class",
		"guild":               "Guild",
		"last_updated":        "Last Updated",
		"last_active_tooltip": "this is an estimate of when the character was last active on the server. to make the timestamp valid as active, if there is a change in the experience percentage and/or the amount of zeny between one scrape and another, it will be considered that the character was active during that time window.",
		"first_99_class":      "First 99 in your class",
		"no_chars_found":      "No characters found matching your criteria.",
		"total_chars":         "(%d total characters)",

		// --- NEW for full_list.html ---
		"full_market_list":    "Full Market List",
		"show":                "Show:",
		"showing_listings":    "Showing <strong>%d</strong> individual listings.",
		"qty_short":           "Qty",
		"price":               "Price",
		"store":               "Store",
		"seller":              "Seller",
		"map":                 "Map",
		"coords":              "Coords",
		"scanned":             "Scanned",
		"availability_status": "Available",
		"yes":                 "Yes",
		"no":                  "No",
		"click_to_copy":       "Click to copy /navi command",
		"copied":              "Copied!",
		"filtering_by_store":  "Filtering by Store:",

		// --- NEW for chat.html ---
		"public_chat_log":        "Public Chat Log",
		"chat_listener_activity": "Chat Listener Activity (Last 24h)",
		"all":                    "All",
		"search_by_message_char": "Search by message or character...",
		"channel":                "Channel",
		"message":                "Message",
		"no_chat_messages":       "No chat messages found.",
		"last_updated_at_chat":   "Last updated: %s", // Different key to avoid conflict
		// --- NEW for guilds.html ---
		"guilds_title":         "Guilds",
		"search_by_guild_name": "Search by Guild Name",
		"level":                "Level",
		"master":               "Master",
		"total_zeny":           "Total Zeny",
		"avg_base_lvl":         "Avg Base Lvl",
		"no_guilds_found":      "No guilds found matching your criteria.",
		"showing_page_guilds":  "Showing page %d of %d (%d total guilds)",

		// --- NEW for guild_detail.html ---
		"guild_detail_title": "%s - Guild Details",
		"led_by":             "Led by",
		"combined_zeny":      "Combined Zeny",
		"guild_members":      "Guild Members",
		"base_short":         "Base",
		"job_short":          "Job",
		"no_chart_data":      "Not enough data to display a chart.",
		"guild_activity_log": "Guild Activity Log",
		"js_num_of_members":  "Number of Members",

		// --- NEW for history.html ---
		"price_history_for":     "Price History:",
		"last_updated_at_hist":  "Last updated: %s %s",
		"no_detailed_info":      "No detailed item information could be found.",
		"item_script":           "Item Script",
		"all_time_price_range":  "All-Time Price Range:",
		"lowest_current_price":  "Lowest Current Price",
		"quantity":              "Quantity:",
		"location":              "Location:",
		"date":                  "Date:",
		"highest_current_price": "Highest Current Price",
		"all_recorded_listings": "All Recorded Listings",
		"qty":                   "Qty",
		"location_coords":       "Location",
		"date_scanned":          "Date Scanned",
		"listing_not_available": "This specific listing is no longer available",
		"total_listings":        "(%d total listings)",
		"js_lowest_price":       "Lowest Price",
		"js_highest_price":      "Highest Price",

		// --- NEW for mvp_kills.html ---
		"mvp_kills_title":   "MVP Kills",
		"showing_chars_mvp": "Showing <strong>%d</strong> characters with MVP kills.",
		"character_name":    "Character Name",
		"total_kills":       "Total Kills",

		// --- NEW for players.html ---
		"online_player_history": "Online Player History",
		"active_players_now":    "Active Players Now",
		"historical_max_active": "Historical Max Active",
		"peak_active_interval":  "Peak Active (%s)",
		"avg_active_interval":   "Avg Active (%s)",
		"low_active_interval":   "Low Active (%s)",
		"interval":              "Interval:",
		"players":               "Players",
		"sellers":               "Sellers",
		"active_delta":          "Active (Δ)",
		"events":                "Events",
		"js_online_players":     "Online Players",
		"js_active_players":     "Active Players (Delta)",
		"js_count":              "Count",
		"js_date_time":          "Date & Time",

		// --- NEW for trading_post.html ---
		"discord_title":    "Discord",
		"all_posts":        "All Posts",
		"selling":          "Selling",
		"buying":           "Buying",
		"both":             "Both",
		"rmt":              "RMT",
		"type":             "Type",
		"item":             "Item",
		"price_ea":         "Price (ea)",
		"payment":          "Payment",
		"discord_user":     "Discord",
		"posted":           "Posted",
		"source":           "Source",
		"type_selling":     "Selling",
		"type_buying":      "Buying",
		"negotiable":       "Negotiable",
		"no_trading_posts": "No trading posts found{{if .SearchQuery}} matching your search{{end}}.",
		"lightbox_title":   "Source / Original Message",

		// --- NEW for woe_rankings.html ---
		"woe_rankings_title": "WoE Rankings",
		"char_rankings":      "Character Rankings",
		"guild_rankings":     "Guild Rankings",
		"search_by_char":     "Search by character name...",
		"search_by_guild":    "Search by guild name...",
		"damage":             "Damage",
		"healing":            "Healing",
		"emperium":           "Emperium",
		"points":             "Points",
		"total_deaths":       "Total Deaths",
		"kd_ratio":           "K/D Ratio",
		"total_damage":       "Total Damage",
		"total_healing":      "Total Healing",

		// --- NEW for xp_calculator.html ---
		"xp_calc_title":    "XP Calculator",
		"error":            "Error:",
		"calc_type":        "Calculator Type",
		"base_level_1_99":  "Base Level (1-99)",
		"job_level_1_50":   "Job Level (1-50)",
		"initial":          "Initial",
		"percentage_0_100": "Percentage (0-100)",
		"final":            "Final",
		"time_spent":       "Time Spent",
		"hours":            "Hours",
		"minutes_0_59":     "Minutes (0-59)",
		"calculate":        "Calculate",
		"results":          "Results",
		"total_xp_gained":  "Total Experience Gained:",
		"xp_per_hour":      "Experience per Hour:",

		// --- NEW for store_detail.html ---
		"store_title":       "Store: %s",
		"store_details":     "Store Details",
		"showing_last_seen": "Showing the <strong>%d</strong> items last seen in this store. Faded items are no longer available.",
		"last_seen":         "Last Seen",
	},
	"pt": {
		"market_summary":       "Resumo do Mercado",
		"search_by_item_name":  "Buscar por nome ou ID do item...",
		"show_only_available":  "Mostrar apenas disponíveis",
		"search":               "Buscar",
		"all_items":            "Todos os Itens",
		"showing_unique_items": "Mostrando <strong>%d</strong> itens únicos.",
		"item_name":            "Nome do Item",
		"item_id":              "ID do Item",
		"available":            "Disponíveis",
		"lowest_price":         "Menor Preço",
		"highest_price":        "Maior Preço",
		"updated_never":        "Atualizado: nunca",
		"updated_ago":          "Atualizado: %s atrás",
		"nav_summary":          "Resumo",
		"nav_full_list":        "Lista Completa",
		"nav_activity":         "Atividade",
		"nav_discord":          "Discord",
		"nav_chat":             "Chat",
		"nav_misc":             "Outros",
		"nav_player_count":     "Jogadores Online",
		"nav_xp_calculator":    "Calculadora XP",
		"nav_about":            "Sobre",
		"nav_rankings":         "Rankings",
		"nav_characters":       "Personagens",
		"nav_guilds":           "Guilds",
		"nav_mvp_kills":        "MVPs Mortos",
		"nav_woe_rankings":     "Rankings WoE",
		// --- NEW for activity.html ---
		"recent_market_activity": "Atividade Recente do Mercado",
		"show_only_sold":         "Mostrar apenas vendidos",
		"filter":                 "Filtrar",
		"clear_filters":          "[Limpar Filtros]",
		"was_added":              "foi adicionado à venda.",
		"no_longer_for_sale":     "não está mais à venda.",
		"was_sold":               "foi vendido.",
		"vendor_logged_off":      "Vendedor deslogou ou item foi vendido",
		"new_historical_low":     "Novo recorde de preço baixo",
		"for":                    "para",
		"no_market_activity":     "Nenhuma atividade de mercado foi registrada ainda.",
		"page_of":                "Página %d de %d",
		"previous":               "Anterior",
		"next":                   "Próxima",

		// --- NEW for about.html ---
		"about_title":           "Sobre",
		"about_what_is_this":    "🏛️ O que é esse site?",
		"about_welcome":         "Bem-vindo! O objetivo deste site é coletar todas as informações públicas do Projeto Yufa, seja no site oficial, no jogo ou no Discord, e organizá-las em um só lugar, de forma fácil de pesquisar e consultar.",
		"about_all_info_public": "Todas as informações aqui são publicamente acessíveis. Porém, algumas delas são difíceis de encontrar ou exigem um grande trabalho para se extrair certas informações. Por esse motivo, criei este site, que está sendo atualizado constantemente.",
		"about_warnings":        "⚠️ Avisos",
		"about_please_read":     "Por favor, leia os seguintes pontos com atenção:",
		"about_warning_1":       "<strong>Não</strong> considere como fonte única da verdade as informações contidas neste site, pois aqui podem existir <strong>inconsistências</strong>, já que ele coleta informações em lugares diferentes, em diferentes horários e dias.",
		"about_warning_2":       "Algumas <strong>presunções são</strong> cruzamentos de dados que o site faz, mas que <strong>também</strong> podem <strong>conter equívocos</strong> (<strong>por exemplo</strong>: se um item foi vendido ou se a loja fechou, a interpretação da IA sobre as mensagens do Discord, a última vez que o personagem esteve ativo, etc.).",
		"about_warning_3":       "As informações podem estar atrasadas. Como o site tira uma \"fotografia\" de diversos estados das suas fontes em diferentes intervalos, podem existir informações desatualizadas que <strong>não</strong> refletem o presente. Sempre verifique o indicador no canto superior direito para ver quando a informação foi atualizada.",
		"about_warning_4":       "<strong>Este</strong> site <strong>é</strong> completamente independente e <strong>não</strong> possui nenhum envolvimento de qualquer pessoa da <strong>administração</strong> do Projeto Yufa. A única <strong>concessão</strong> por parte dos administradores foi a de <strong>não</strong> bloquear o acesso deste site ao site oficial do Projeto Yufa. <strong>Caso</strong> eles entendam que este site prejudica de alguma maneira o servidor, a comunidade ou o <strong>funcionamento</strong> do Projeto Yufa, <strong>retirarei</strong> o site do ar prontamente.",
		"info_loaded_at":        "Informações carregadas: %s",

		// --- NEW for character_detail.html ---
		"last_updated_at":  "Última atualização: %s",
		"char_info":        "Informações do Personagem",
		"rank":             "Rank",
		"base_level":       "Nível de Base",
		"job_level":        "Nível de Classe",
		"experience":       "Experiência",
		"zeny":             "Zeny",
		"status":           "Status",
		"active":           "Ativo",
		"inactive":         "Inativo",
		"last_active":      "Visto por último",
		"mvp_kills_total":  "MVPs Mortos (%d Total)",
		"kills":            "mortes",
		"no_mvp_kills":     "Este jogador não tem mortes de MVP registradas.",
		"char_changelog":   "Histórico do Personagem",
		"timestamp":        "Data/Hora",
		"activity":         "Atividade",
		"first":            "Primeira",
		"last":             "Última",
		"no_char_activity": "Nenhuma atividade recente registrada para este personagem.",
		"guild_info":       "Informações da Guild",
		"guild_name":       "Nome da Guild",
		"guild_level":      "Nível da Guild",
		"guild_master":     "Líder da Guild",
		"members":          "Membros",
		"not_in_guild":     "Este personagem não está em uma guild.",
		"guild_history":    "Histórico de Guild",
		"no_guild_history": "Nenhum histórico de guild registrado.",
		"guild_leader":     "Líder da Guild",

		// --- NEW for character_changelog.html ---
		"char_changelog_title": "Histórico de Personagens",
		"character":            "Personagem",
		"no_changelog_entries": "Nenhuma entrada de histórico encontrada.",
		"prev_short":           "Ant",
		"next_short":           "Próx",
		// --- NEW for characters.html ---
		"characters_title":    "Personagens",
		"search_by_name":      "Buscar por Nome",
		"filter_by_class":     "Filtrar por Classe",
		"all_classes":         "Todas as Classes",
		"apply":               "Aplicar",
		"show_columns":        "Mostrar Colunas:",
		"filtering_by_guild":  "Filtrando por Guild:",
		"clear_filter":        "[Limpar Filtro]",
		"stats_distribution":  "📊 Estatísticas e Distribuição",
		"class_distribution":  "Distribuição de Classes",
		"show_types":          "Mostrar Tipos:",
		"novice":              "Aprendiz",
		"first_class":         "Classe 1",
		"second_class":        "Classe 2",
		"total_zeny_filtered": "Zeny Total (Filtrado)",
		"sum_across_chars":    "Soma entre %d personagens (sem banco).",
		"name":                "Nome",
		"base_lvl":            "Nível Base",
		"job_lvl":             "Nível Classe",
		"exp_perc":            "Exp %",
		"class":               "Classe",
		"guild":               "Guild",
		"last_updated":        "Última Atualização",
		"last_active_tooltip": "esta é uma estimativa de quando o personagem esteve ativo pela última vez no servidor. para que o timestamp seja válido como ativo, se houver mudança no percentual de experiência e/ou na quantidade de zeny entre um scrape e outro, será considerado que o personagem esteve ativo nessa janela de tempo.",
		"first_99_class":      "Primeiro 99 da sua classe",
		"no_chars_found":      "Nenhum personagem encontrado com seus critérios.",
		"total_chars":         "(%d personagens no total)",

		// --- NEW for full_list.html ---
		"full_market_list":    "Lista Completa do Mercado",
		"show":                "Mostrar:",
		"showing_listings":    "Mostrando <strong>%d</strong> itens individuais.",
		"qty_short":           "Qtd",
		"price":               "Preço",
		"store":               "Loja",
		"seller":              "Vendedor",
		"map":                 "Mapa",
		"coords":              "Coords",
		"scanned":             "Visto em",
		"availability_status": "Disponível",
		"yes":                 "Sim",
		"no":                  "Não",
		"click_to_copy":       "Clique para copiar comando /navi",
		"copied":              "Copiado!",
		"filtering_by_store":  "Filtrando por Loja:",

		// --- NEW for chat.html ---
		"public_chat_log":        "Log de Chat Público",
		"chat_listener_activity": "Atividade do Chat (Últimas 24h)",
		"all":                    "Todos",
		"search_by_message_char": "Buscar por mensagem ou personagem...",
		"channel":                "Canal",
		"message":                "Mensagem",
		"no_chat_messages":       "Nenhuma mensagem de chat encontrada.",
		"last_updated_at_chat":   "Última atualização: %s",

		// --- NEW for guilds.html ---
		"guilds_title":         "Guilds",
		"search_by_guild_name": "Buscar por Nome da Guild",
		"level":                "Nível",
		"master":               "Líder",
		"total_zeny":           "Zeny Total",
		"avg_base_lvl":         "Nível Base Médio",
		"no_guilds_found":      "Nenhuma guild encontrada com seus critérios.",
		"showing_page_guilds":  "Mostrando página %d de %d (%d guilds no total)",

		// --- NEW for guild_detail.html ---
		"guild_detail_title": "%s - Detalhes da Guild",
		"led_by":             "Liderada por",
		"combined_zeny":      "Zeny Combinado",
		"guild_members":      "Membros da Guild",
		"base_short":         "Base",
		"job_short":          "Classe",
		"no_chart_data":      "Sem dados suficientes para exibir um gráfico.",
		"guild_activity_log": "Histórico de Atividade da Guild",
		"js_num_of_members":  "Número de Membros",

		// --- NEW for history.html ---
		"price_history_for":     "Histórico de Preço:",
		"last_updated_at_hist":  "Última atualização: %s %s",
		"no_detailed_info":      "Nenhuma informação detalhada do item foi encontrada.",
		"item_script":           "Script do Item",
		"all_time_price_range":  "Faixa de Preço Histórica:",
		"lowest_current_price":  "Menor Preço Atual",
		"quantity":              "Quantidade:",
		"location":              "Localização:",
		"date":                  "Data:",
		"highest_current_price": "Maior Preço Atual",
		"all_recorded_listings": "Todos os Anúncios Registrados",
		"qty":                   "Qtd",
		"location_coords":       "Localização",
		"date_scanned":          "Data da Verificação",
		"listing_not_available": "Este anúncio específico não está mais disponível",
		"total_listings":        "(%d anúncios no total)",
		"js_lowest_price":       "Menor Preço",
		"js_highest_price":      "Maior Preço",

		// --- NEW for mvp_kills.html ---
		"mvp_kills_title":   "MVPs Mortos",
		"showing_chars_mvp": "Mostrando <strong>%d</strong> personagens com mortes de MVP.",
		"character_name":    "Nome do Personagem",
		"total_kills":       "Total de Mortes",

		// --- NEW for players.html ---
		"online_player_history": "Histórico de Jogadores Online",
		"active_players_now":    "Jogadores Ativos Agora",
		"historical_max_active": "Máx. Histórico de Ativos",
		"peak_active_interval":  "Pico de Ativos (%s)",
		"avg_active_interval":   "Média de Ativos (%s)",
		"low_active_interval":   "Mínimo de Ativos (%s)",
		"interval":              "Intervalo:",
		"players":               "Jogadores",
		"sellers":               "Vendedores",
		"active_delta":          "Ativos (Δ)",
		"events":                "Eventos",
		"js_online_players":     "Jogadores Online",
		"js_active_players":     "Jogadores Ativos (Delta)",
		"js_count":              "Contagem",
		"js_date_time":          "Data e Hora",

		// --- NEW for trading_post.html ---
		"discord_title":    "Discord",
		"all_posts":        "Todos os Posts",
		"selling":          "Vendendo",
		"buying":           "Comprando",
		"both":             "Ambos",
		"rmt":              "RMT",
		"type":             "Tipo",
		"item":             "Item",
		"price_ea":         "Preço (un)",
		"payment":          "Pgto",
		"discord_user":     "Discord",
		"posted":           "Postado",
		"source":           "Fonte",
		"type_selling":     "Vendendo",
		"type_buying":      "Comprando",
		"negotiable":       "Negociável",
		"no_trading_posts": "Nenhum post encontrado{{if .SearchQuery}} com a sua busca{{end}}.",
		"lightbox_title":   "Fonte / Mensagem Original",

		// --- NEW for woe_rankings.html ---
		"woe_rankings_title": "Rankings WoE",
		"char_rankings":      "Rankings de Personagens",
		"guild_rankings":     "Rankings de Guilds",
		"search_by_char":     "Buscar por nome de personagem...",
		"search_by_guild":    "Buscar por nome de guild...",
		"damage":             "Dano",
		"healing":            "Cura",
		"emperium":           "Emperium",
		"points":             "Pontos",
		"total_deaths":       "Total de Mortes",
		"kd_ratio":           "K/D",
		"total_damage":       "Dano Total",
		"total_healing":      "Cura Total",

		// --- NEW for xp_calculator.html ---
		"xp_calc_title":    "Calculadora XP",
		"error":            "Erro:",
		"calc_type":        "Tipo de Calculadora",
		"base_level_1_99":  "Nível de Base (1-99)",
		"job_level_1_50":   "Nível de Classe (1-50)",
		"initial":          "Inicial",
		"percentage_0_100": "Porcentagem (0-100)",
		"final":            "Final",
		"time_spent":       "Tempo Gasto",
		"hours":            "Horas",
		"minutes_0_59":     "Minutos (0-59)",
		"calculate":        "Calcular",
		"results":          "Resultados",
		"total_xp_gained":  "Total de Experiência Ganhos:",
		"xp_per_hour":      "Experiência por Hora:",

		// --- NEW for store_detail.html ---
		"store_title":       "Loja: %s",
		"store_details":     "Detalhes da Loja",
		"showing_last_seen": "Mostrando os <strong>%d</strong> itens vistos por último nesta loja. Itens esmaecidos não estão mais disponíveis.",
		"last_seen":         "Visto por Último",
	},
}

// --- NEW: getTranslations returns the correct map ---
func getTranslations(lang string) map[string]string {
	if trans, ok := translations[lang]; ok {
		return trans
	}
	// Default to English
	return translations["en"]
}

// --- NEW: getLang reads the language from the cookie ---
func getLang(r *http.Request) string {
	cookie, err := r.Cookie("lang")
	if err != nil {
		// No cookie, default to Portuguese
		return "pt"
	}
	if cookie.Value == "en" {
		return "en"
	}
	// Default to Portuguese for "pt" or any other value
	return "pt"
}

// --- NEW: setLangHandler sets the cookie and redirects ---
func setLangHandler(w http.ResponseWriter, r *http.Request) {
	lang := r.URL.Query().Get("lang")
	redirectURL := r.URL.Query().Get("redirect")

	if lang != "en" && lang != "pt" {
		lang = "pt" // Default to 'pt'
	}

	if redirectURL == "" {
		redirectURL = "/" // Default redirect to home
	}

	// Set the cookie
	http.SetCookie(w, &http.Cookie{
		Name:     "lang",
		Value:    lang,
		Path:     "/",
		Expires:  time.Now().Add(365 * 24 * time.Hour), // Cookie good for 1 year
		HttpOnly: true,
		Secure:   r.TLS != nil,
		SameSite: http.SameSiteLaxMode,
	})

	// Redirect back to the page the user was on
	http.Redirect(w, r, redirectURL, http.StatusSeeOther)
}

type ItemSearchResult struct {
	ID       int    `json:"ID"`
	Name     string `json:"Name"`
	ImageURL string `json:"ImageURL"`
}

var definedEvents = []EventDefinition{
	{
		Name:      "Battlegrounds",
		StartTime: "20:00",
		EndTime:   "21:00",
		Days:      []time.Weekday{time.Monday, time.Tuesday, time.Wednesday, time.Thursday, time.Friday, time.Saturday, time.Sunday},
	},
	{
		Name:      "War of Emperium",
		StartTime: "21:00",
		EndTime:   "22:00",
		Days:      []time.Weekday{time.Sunday},
	},
}

var (
	nameSanitizer = regexp.MustCompile(`[^a-zA-Z0-9 ]+`)

	contactSanitizer = regexp.MustCompile(`[^a-zA-Z0-9\s:.,#@-]+`)

	notesSanitizer = regexp.MustCompile(`[^a-zA-Z0-9\s.,?!'-]+`)

	itemSanitizer = regexp.MustCompile(`[^\p{L}0-9\s\[\]\+\-]+`)

	reCardRemover = regexp.MustCompile(`(?i)\s*\b(card|carta)\b\s*`)

	reSlotRemover = regexp.MustCompile(`\s*\[\d+\]\s*`)
)

var classImages = map[string]string{
	"Aprendiz":       "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/8/8b/Icon_jobs_0.png",
	"Super Aprendiz": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/c/c7/Icon_jobs_4001.png",
	"Arqueiro":       "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/9/97/Icon_jobs_3.png",
	"Espadachim":     "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/5/5b/Icon_jobs_1.png",
	"Gatuno":         "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/3/3c/Icon_jobs_6.png",
	"Mago":           "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/9/99/Icon_jobs_2.png",
	"Mercador":       "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/9/9e/Icon_jobs_5.png",
	"Noviço":         "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/c/c5/Icon_jobs_4.png",
	"Alquimista":     "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/c/c7/Icon_jobs_18.png",
	"Arruaceiro":     "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/4/48/Icon_jobs_17.png",
	"Bardo":          "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/6/69/Icon_jobs_19.png",
	"Bruxo":          "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/0/09/Icon_jobs_9.png",
	"Cavaleiro":      "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/1/1d/Icon_jobs_7.png",
	"Caçador":        "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/e/eb/Icon_jobs_11.png",
	"Ferreiro":       "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/7/7b/Icon_jobs_10.png",
	"Mercenário":     "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/9/9c/Icon_jobs_12.png",
	"Monge":          "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/4/44/Icon_jobs_15.png",
	"Odalisca":       "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/d/dc/Icon_jobs_20.png",
	"Sacerdote":      "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/3/3a/Icon_jobs_8.png",
	"Sábio":          "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/0/0e/Icon_jobs_16.png",
	"Templário":      "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/e/e1/Icon_jobs_14.png",
}

const MvpKillCountOffset = 3

var templateFuncs = template.FuncMap{
	"lower": strings.ToLower,
	"cleanCardName": func(cardName string) string {
		return strings.TrimSpace(reCardRemover.ReplaceAllString(cardName, " "))
	},
	"toggleOrder": func(currentOrder string) string {
		if currentOrder == "ASC" {
			return "DESC"
		}
		return "ASC"
	},
	"parseDropMessage": func(msg string) map[string]string {
		// dropMessageRegex is defined in scraper.go in the package-level var block
		matches := dropMessageRegex.FindStringSubmatch(msg)
		if len(matches) == 4 { // Check for 4 matches
			// matches[0] = full string
			// matches[1] = character name (e.g., "Lindinha GC")
			// matches[2] = "got" or "stole"
			// matches[3] = rest of message (e.g., "Raydric's Iron Cain (chance: 0.01%)")
			return map[string]string{
				"charName": matches[1],
				"message":  matches[2] + " " + matches[3], // Reconstruct "got Item..." or "stole Item..."
			}
		}
		return nil
	},
	"formatZeny": func(zeny int64) string {
		s := strconv.FormatInt(zeny, 10)
		if len(s) <= 3 {
			return s
		}
		var result []string
		for i := len(s); i > 0; i -= 3 {
			start := i - 3
			if start < 0 {
				start = 0
			}
			result = append([]string{s[start:i]}, result...)
		}
		return strings.Join(result, ".")
	},
	"formatRMT": func(rmt int64) string {

		return fmt.Sprintf("R$ %d", rmt)
	},
	"getKillCount": func(kills map[string]int, mobID string) int {
		return kills[mobID]
	},
	"formatAvgLevel": func(level float64) string {
		if level == 0 {
			return "N/A"
		}
		return fmt.Sprintf("%.1f", level)
	},
	"getClassImageURL": func(class string) string {
		if url, ok := classImages[class]; ok {
			return url
		}
		// Fallback icon (Aprendiz)
		return "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/8/8b/Icon_jobs_0.png"
	},
	"TmplHTML": func(s string) template.HTML {
		return template.HTML(s)
	},
	"dict": func(values ...interface{}) (map[string]interface{}, error) {
		if len(values)%2 != 0 {
			return nil, fmt.Errorf("invalid dict call: odd number of arguments")
		}
		dict := make(map[string]interface{}, len(values)/2)
		for i := 0; i < len(values); i += 2 {
			key, ok := values[i].(string)
			if !ok {
				return nil, fmt.Errorf("dict keys must be strings")
			}
			dict[key] = values[i+1]
		}
		return dict, nil
	},
}

type PaginationData struct {
	CurrentPage int
	TotalPages  int
	PrevPage    int
	NextPage    int
	HasPrevPage bool
	HasNextPage bool
	Offset      int
}

func newPaginationData(r *http.Request, totalItems int, itemsPerPage int) PaginationData {
	pageStr := r.FormValue("page")
	page, err := strconv.Atoi(pageStr)
	if err != nil || page < 1 {
		page = 1
	}

	pd := PaginationData{}
	if totalItems > 0 {
		pd.TotalPages = int(math.Ceil(float64(totalItems) / float64(itemsPerPage)))
	} else {
		pd.TotalPages = 1
	}

	if page > pd.TotalPages {
		page = pd.TotalPages
	}
	if page < 1 {
		page = 1
	}

	pd.CurrentPage = page
	pd.Offset = (page - 1) * itemsPerPage
	pd.PrevPage = page - 1
	pd.NextPage = page + 1
	pd.HasPrevPage = page > 1
	pd.HasNextPage = page < pd.TotalPages
	return pd
}

func getSortClause(r *http.Request, allowedSorts map[string]string, defaultSortBy, defaultOrder string) (string, string, string) {
	sortBy := r.FormValue("sort_by")
	order := r.FormValue("order")

	orderByColumn, ok := allowedSorts[sortBy]
	if !ok {
		sortBy = defaultSortBy
		order = defaultOrder
		orderByColumn = allowedSorts[sortBy]
	}

	if strings.ToUpper(order) != "ASC" && strings.ToUpper(order) != "DESC" {
		order = defaultOrder
	}

	return fmt.Sprintf("ORDER BY %s %s", orderByColumn, order), sortBy, order
}

func buildItemSearchClause(searchQuery, tableAlias string) (string, []interface{}, error) {
	if searchQuery == "" {
		return "", nil, nil
	}

	alias := strings.Trim(regexp.MustCompile(`[^a-zA-Z0-9_]`).ReplaceAllString(tableAlias, ""), ".")
	if alias != "" {
		alias += "."
	}

	if _, err := strconv.Atoi(searchQuery); err == nil {
		return fmt.Sprintf("%sitem_id = ?", alias), []interface{}{searchQuery}, nil
	}

	idList, err := getCombinedItemIDs(searchQuery)
	if err != nil {
		return "", nil, fmt.Errorf("failed to perform combined item search: %w", err)
	}

	if len(idList) > 0 {
		placeholders := strings.Repeat("?,", len(idList)-1) + "?"
		clause := fmt.Sprintf("%sitem_id IN (%s)", alias, placeholders)
		params := make([]interface{}, len(idList))
		for i, id := range idList {
			params[i] = id
		}
		return clause, params, nil
	}

	return "1 = 0", nil, nil
}

func renderTemplate(w http.ResponseWriter, r *http.Request, tmplFile string, data interface{}) {

	tmpl, ok := templateCache[tmplFile]
	if !ok {
		log.Printf("[E] [HTTP] Could not find template '%s' in cache!", tmplFile)
		http.Error(w, "Could not load template", http.StatusInternalServerError)
		return
	}

	// --- NEW: Create page context ---
	lang := getLang(r)
	pageCtx := BasePageData{
		Lang:       lang,
		T:          getTranslations(lang),
		RequestURL: r.URL.RequestURI(),
	}

	// Wrap the page-specific data and the base context in a single map
	fullData := map[string]interface{}{
		"Page": pageCtx, // Site-wide context
		"Data": data,    // Page-specific data (e.g., SummaryPageData)
	}
	// --- END NEW ---

	// Execute with the new wrapped data
	err := tmpl.Execute(w, fullData)
	if err != nil {
		log.Printf("[E] [HTTP] Could not execute template '%s': %v", tmplFile, err)
	}
}

func sanitizeString(input string, sanitizer *regexp.Regexp) string {
	return sanitizer.ReplaceAllString(input, "")
}

func getCombinedItemIDs(searchQuery string) ([]int, error) {
	var wg sync.WaitGroup
	scrapedIDsChan := make(chan []int, 1)
	localIDsChan := make(chan []int, 1)

	wg.Add(2)

	// Goroutine 1: Scrape rodatabase.com (unchanged)
	go func() {
		defer wg.Done()

		results, err := scrapeRODatabaseSearch(searchQuery, 0)
		if err != nil {
			log.Printf("[W] [HTTP] Concurrent scrape failed for '%s': %v", searchQuery, err)
			scrapedIDsChan <- []int{}
			return
		}

		var ids []int
		if results != nil {
			for _, res := range results {
				ids = append(ids, res.ID)
			}
		}
		scrapedIDsChan <- ids
	}()

	// Goroutine 2: Search local database (OPTIMIZED)
	go func() {
		defer wg.Done()
		var ids []int

		// --- OPTIMIZATION ---
		// The original query included a UNION with the `items` table using LIKE.
		// The `items` table is a transaction log and can be massive, making
		// a `LIKE` query extremely slow and inefficient.
		// The `internal_item_db` is the canonical source for item names and IDs.
		// This query is now targeted *only* at the fast, indexed `internal_item_db`.
		const query = `
			SELECT item_id FROM internal_item_db
			WHERE name LIKE ? OR name_pt LIKE ?
		`
		// --- END OPTIMIZATION ---

		likeQuery := "%" + searchQuery + "%"
		rows, err := db.Query(query, likeQuery, likeQuery)

		if err != nil {
			log.Printf("[W] [HTTP] Concurrent local ID search failed for '%s': %v", searchQuery, err)
			localIDsChan <- []int{}
			return
		}
		defer rows.Close()
		for rows.Next() {
			var id int
			if err := rows.Scan(&id); err == nil {
				ids = append(ids, id)
			}
		}
		localIDsChan <- ids
	}()

	wg.Wait()
	scrapedIDs := <-scrapedIDsChan
	localIDs := <-localIDsChan
	close(scrapedIDsChan)
	close(localIDsChan)

	// Combine and de-duplicate (unchanged)
	combinedIDs := make(map[int]struct{})
	for _, id := range scrapedIDs {
		combinedIDs[id] = struct{}{}
	}
	for _, id := range localIDs {
		combinedIDs[id] = struct{}{}
	}

	if len(combinedIDs) == 0 {
		return []int{}, nil
	}

	idList := make([]int, 0, len(combinedIDs))
	for id := range combinedIDs {
		idList = append(idList, id)
	}
	return idList, nil
}

func getItemTypeTabs() []ItemTypeTab {
	var itemTypes []ItemTypeTab
	// --- MODIFICATION: Query internal_item_db (using the 'type' column) ---
	rows, err := db.Query("SELECT DISTINCT type FROM internal_item_db WHERE type IS NOT NULL AND type != '' ORDER BY type ASC")
	// --- END MODIFICATION ---
	if err != nil {
		log.Printf("[W] [HTTP] Could not query for item types: %v", err)
		return itemTypes
	}
	defer rows.Close()

	for rows.Next() {
		var itemType string
		if err := rows.Scan(&itemType); err != nil {
			log.Printf("[W] [HTTP] Failed to scan item type: %v", err)
			continue
		}
		// --- MODIFICATION: The mapItemTypeToTabData function expects "CamelCase"
		// The internal_item_db stores "Usable", "Etc", "Weapon", "Armor".
		// We will map these.
		var mappedType string
		switch strings.ToLower(itemType) {
		case "healing":
			mappedType = "Healing Item"
		case "usable":
			mappedType = "Usable Item"
		case "etc":
			mappedType = "Miscellaneous"
		case "ammo":
			mappedType = "Ammunition"
		case "card":
			mappedType = "Card"
		case "petegg":
			mappedType = "Monster Egg"
		case "petarmor":
			mappedType = "Pet Armor"
		case "petequip":
			mappedType = "Pet Armor"
		case "weapon":
			mappedType = "Weapon"
		case "armor":
			mappedType = "Armor"
		case "shadowgear":
			mappedType = "Armor" // Grouping shadow gear with armor
		case "cash":
			mappedType = "Cash Shop Item"
		default:
			mappedType = itemType // Fallback
		}
		itemTypes = append(itemTypes, mapItemTypeToTabData(mappedType))
		// --- END MODIFICATION ---
	}
	return itemTypes
}

func generateEventIntervals(viewStart, viewEnd time.Time, events []EventDefinition, activeDates map[string]struct{}) []map[string]interface{} {
	var intervals []map[string]interface{}
	loc := viewStart.Location()
	currentDay := time.Date(viewStart.Year(), viewStart.Month(), viewStart.Day(), 0, 0, 0, 0, loc)

	for currentDay.Before(viewEnd) {
		dateStr := currentDay.Format("2006-01-02")
		if _, exists := activeDates[dateStr]; !exists {
			currentDay = currentDay.Add(24 * time.Hour)
			continue
		}

		for _, event := range events {
			isEventDay := false
			for _, dayOfWeek := range event.Days {
				if currentDay.Weekday() == dayOfWeek {
					isEventDay = true
					break
				}
			}

			if isEventDay {
				eventStartStr := fmt.Sprintf("%s %s", currentDay.Format("2006-01-02"), event.StartTime)
				eventEndStr := fmt.Sprintf("%s %s", currentDay.Format("2006-01-02"), event.EndTime)

				eventStart, err1 := time.ParseInLocation("2006-01-02 15:04", eventStartStr, loc)
				eventEnd, err2 := time.ParseInLocation("2006-01-02 15:04", eventEndStr, loc)

				if err1 != nil || err2 != nil {
					continue
				}

				if eventStart.Before(viewEnd) && eventEnd.After(viewStart) {
					intervals = append(intervals, map[string]interface{}{
						"name":  event.Name,
						"start": eventStart.Format("2006-01-02 15:04"),
						"end":   eventEnd.Format("2006-01-02 15:04"),
					})
				}
			}
		}
		currentDay = currentDay.Add(24 * time.Hour)
	}
	return intervals
}

func mapItemTypeToTabData(typeName string) ItemTypeTab {
	tab := ItemTypeTab{FullName: typeName, ShortName: typeName, IconItemID: 909}
	switch typeName {
	case "Ammunition":
		tab.ShortName = ""
		tab.IconItemID = 1750
	case "Armor":
		tab.ShortName = ""
		tab.IconItemID = 2316
	case "Card":
		tab.ShortName = ""
		tab.IconItemID = 4133
	case "Delayconsume":
		tab.ShortName = ""
		tab.IconItemID = 610
	case "Healing Item":
		tab.ShortName = ""
		tab.IconItemID = 501
	case "Miscellaneous":
		tab.ShortName = ""
		tab.IconItemID = 909
	case "Monster Egg":
		tab.ShortName = ""
		tab.IconItemID = 9001
	case "Pet Armor":
		tab.ShortName = ""
		tab.IconItemID = 5183
	case "Taming Item":
		tab.ShortName = ""
		tab.IconItemID = 632
	case "Usable Item":
		tab.ShortName = ""
		tab.IconItemID = 603
	case "Weapon":
		tab.ShortName = ""
		tab.IconItemID = 1162
	case "Cash Shop Item":
		tab.ShortName = ""
		tab.IconItemID = 200441

	}
	return tab
}

var templateCache = make(map[string]*template.Template)

func init() {

	templates := []string{
		"index.html",
		"full_list.html",
		"activity.html",
		"history.html",
		"players.html",
		"characters.html",
		"guilds.html",
		"mvp_kills.html",
		"character_detail.html",
		"character_changelog.html",
		"guild_detail.html",
		"store_detail.html",
		"trading_post.html",
		"woe_rankings.html",
		"chat.html",
		"xp_calculator.html",
		"about.html",
	}

	// --- MODIFICATION START ---
	// Define which common files to include.
	navbarPath := "navbar.html"
	paginationPath := "pagination.html" // <-- ADD THIS

	// Define which templates need which partials.
	// All templates get the navbar.
	templatesWithPagination := map[string]bool{
		"activity.html":            true,
		"character_changelog.html": true,
		"character_detail.html":    true,
		"characters.html":          true,
		"chat.html":                true,
		"guild_detail.html":        true,
		"guilds.html":              true,
		"history.html":             true,
	}

	log.Println("[I] [HTTP] Parsing all application templates...")
	for _, tmplName := range templates {

		// Start with the base files
		filesToParse := []string{tmplName, navbarPath}

		// Add pagination only if this template needs it
		if templatesWithPagination[tmplName] {
			filesToParse = append(filesToParse, paginationPath) // <-- ADD THIS
		}

		tmpl, err := template.New(tmplName).Funcs(templateFuncs).ParseFiles(filesToParse...)
		if err != nil {
			// --- REFACTOR ---
			// The old code had a complex if/else if block here to check for
			// "admin_edit_post.html" and "admin.html".
			//
			// Those files are NOT in the 'templates' list above, so that
			// error-handling code was unreachable dead code.
			//
			// We can safely remove it, leaving only the fatal error check.
			// Admin templates are parsed on-demand in their own handlers.
			log.Fatalf("[F] [HTTP] Could not parse template '%s': %v", tmplName, err)
			// --- END REFACTOR ---
		}

		templateCache[tmplName] = tmpl
	}
	log.Println("[I] [HTTP] All templates parsed and cached successfully.")
	// --- MODIFICATION END ---
}

func summaryHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)
		return
	}
	searchQuery := r.FormValue("query")
	selectedType := r.FormValue("type")

	formSubmitted := len(r.Form) > 0
	showAll := false
	if formSubmitted && r.FormValue("only_available") != "true" {
		showAll = true
	}

	var innerWhereConditions []string
	var innerParams []interface{}
	var outerWhereConditions []string
	var outerParams []interface{}

	// 1. Item search (name/ID) filters the 'items' table directly.
	if searchClause, searchParams, err := buildItemSearchClause(searchQuery, "i"); err != nil {
		http.Error(w, "Failed to build item search query", http.StatusInternalServerError)
		return
	} else if searchClause != "" {
		innerWhereConditions = append(innerWhereConditions, searchClause)
		innerParams = append(innerParams, searchParams...)
	}

	// 2. Type filter checks 'internal_item_db'.
	if selectedType != "" {
		var dbType string
		switch selectedType {
		case "Healing Item":
			dbType = "Healing"
		case "Usable Item":
			dbType = "Usable"
		case "Miscellaneous":
			dbType = "Etc"
		case "Ammunition":
			dbType = "Ammo"
		case "Card":
			dbType = "Card"
		case "Monster Egg":
			dbType = "PetEgg"
		case "Pet Armor":
			dbType = "PetArmor"
		case "Weapon":
			dbType = "Weapon"
		case "Armor":
			dbType = "Armor"
		case "Cash Shop Item":
			dbType = "Cash"
		default:
			dbType = selectedType
		}
		outerWhereConditions = append(outerWhereConditions, "local_db.type = ?")
		outerParams = append(outerParams, dbType)
	}

	// --- FIX: Availability filter now goes into the OUTER WHERE clause ---
	if !showAll {
		// It filters on 't', the alias for the inner subquery.
		outerWhereConditions = append(outerWhereConditions, "t.listing_count > 0")
	}
	// --- END FIX ---

	// Build clause strings
	innerWhereClause := ""
	if len(innerWhereConditions) > 0 {
		innerWhereClause = "WHERE " + strings.Join(innerWhereConditions, " AND ")
	}
	outerWhereClause := ""
	if len(outerWhereConditions) > 0 {
		// This will now correctly prepend "WHERE"
		// e.g., "WHERE local_db.type = ? AND t.listing_count > 0"
		outerWhereClause = "WHERE " + strings.Join(outerWhereConditions, " AND ")
	}

	// --- FIX: The separate 'havingClause' variable has been removed ---

	// --- FIX: Restructured COUNT query to use outerWhereClause ---
	countQuery := `
		SELECT COUNT(*)
		FROM (
			SELECT 1
			FROM (
				SELECT
					MAX(i.item_id) as item_id,
					SUM(CASE WHEN i.is_available = 1 THEN 1 ELSE 0 END) as listing_count
				FROM items i
				%s -- innerWhereClause
				GROUP BY i.name_of_the_item
			) AS t
			LEFT JOIN internal_item_db local_db ON t.item_id = local_db.item_id
			%s -- outerWhereClause
		) AS UniqueItems
	`
	// --- END FIX ---

	var totalUniqueItems int
	// Combine params for count query
	countParams := append(innerParams, outerParams...)
	err := db.QueryRow(fmt.Sprintf(countQuery, innerWhereClause, outerWhereClause), countParams...).Scan(&totalUniqueItems)
	if err != nil {
		log.Printf("[E] [HTTP] Summary count query error: %v", err)
		totalUniqueItems = 0
	}

	// --- FIX: Restructured main SELECT query ---
	selectQuery := `
        SELECT
            t.name_of_the_item,
            local_db.name_pt,
            t.item_id,
            t.lowest_price,
            t.highest_price,
            t.listing_count
        FROM (
            SELECT
                i.name_of_the_item,
                MAX(i.item_id) as item_id, 
                MIN(CASE WHEN i.is_available = 1 THEN CAST(REPLACE(i.price, ',', '') AS INTEGER) ELSE NULL END) as lowest_price,
                MAX(CASE WHEN i.is_available = 1 THEN CAST(REPLACE(i.price, ',', '') AS INTEGER) ELSE NULL END) as highest_price,
                SUM(CASE WHEN i.is_available = 1 THEN 1 ELSE 0 END) as listing_count
            FROM items i
            %s -- innerWhereClause
            GROUP BY i.name_of_the_item
        ) AS t
        LEFT JOIN internal_item_db local_db ON t.item_id = local_db.item_id
        %s -- outerWhereClause
    `
	// --- END FIX ---

	allowedSorts := map[string]string{
		"name":          "t.name_of_the_item",
		"item_id":       "t.item_id",
		"listings":      "t.listing_count",
		"lowest_price":  "t.lowest_price",
		"highest_price": "t.highest_price",
	}
	orderByClause, sortBy, order := getSortClause(r, allowedSorts, "highest_price", "DESC")

	query := fmt.Sprintf("%s %s, t.name_of_the_item ASC;",
		fmt.Sprintf(selectQuery, innerWhereClause, outerWhereClause),
		orderByClause,
	)

	// Combine params for main query
	mainParams := append(innerParams, outerParams...)
	rows, err := db.Query(query, mainParams...)
	if err != nil {
		log.Printf("[E] [HTTP] Summary query error: %v, Query: %s, Params: %v", err, query, mainParams)
		http.Error(w, "Database query for summary failed", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var items []ItemSummary
	for rows.Next() {
		var item ItemSummary
		if err := rows.Scan(&item.Name, &item.NamePT, &item.ItemID, &item.LowestPrice, &item.HighestPrice, &item.ListingCount); err != nil {
			log.Printf("[W] [HTTP] Failed to scan summary row: %v", err)
			continue
		}
		items = append(items, item)
	}

	var totalVisitors int
	db.QueryRow("SELECT COUNT(*) FROM visitors").Scan(&totalVisitors)

	data := SummaryPageData{
		Items:            items,
		SearchQuery:      searchQuery,
		SortBy:           sortBy,
		Order:            order,
		ShowAll:          showAll,
		LastScrapeTime:   GetLastScrapeTime(),
		ItemTypes:        getItemTypeTabs(),
		SelectedType:     selectedType,
		TotalVisitors:    totalVisitors,
		TotalUniqueItems: totalUniqueItems,
		PageTitle:        "Summary",
	}
	renderTemplate(w, r, "index.html", data)
}

func fullListHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)
		return
	}
	searchQuery := r.FormValue("query")
	storeNameQuery := r.FormValue("store_name")
	selectedCols := r.Form["cols"]
	selectedType := r.FormValue("type")

	formSubmitted := len(r.Form) > 0
	showAll := false
	if formSubmitted && r.FormValue("only_available") != "true" {
		showAll = true
	}

	var allStoreNames []string
	storeRows, err := db.Query("SELECT DISTINCT store_name FROM items WHERE is_available = 1 ORDER BY store_name ASC")
	if err != nil {
		log.Printf("[W] [HTTP] Could not query for store names: %v", err)
	} else {
		defer storeRows.Close()
		for storeRows.Next() {
			var storeName string
			if err := storeRows.Scan(&storeName); err != nil {
				log.Printf("[W] [HTTP] Failed to scan store name: %v", err)
				continue
			}
			allStoreNames = append(allStoreNames, storeName)
		}
	}

	allCols := []Column{
		{ID: "item_id", DisplayName: "Item ID"}, {ID: "quantity", DisplayName: "Quantity"},
		{ID: "store_name", DisplayName: "Store Name"}, {ID: "seller_name", DisplayName: "Seller Name"},
		{ID: "map_name", DisplayName: "Map Name"}, {ID: "map_coordinates", DisplayName: "Map Coords"},
		{ID: "retrieved", DisplayName: "Date Retrieved"},
	}
	visibleColumns := make(map[string]bool)
	columnParams := url.Values{}

	if len(selectedCols) > 0 {
		for _, col := range selectedCols {
			visibleColumns[col] = true
			columnParams.Add("cols", col)
		}
	} else {
		visibleColumns["quantity"] = true
		visibleColumns["store_name"] = true
		visibleColumns["map_coordinates"] = true
	}

	allowedSorts := map[string]string{
		"name": "i.name_of_the_item", "item_id": "i.item_id", "quantity": "i.quantity",
		"price": "CAST(REPLACE(i.price, ',', '') AS INTEGER)", "store": "i.store_name", "seller": "i.seller_name",
		"retrieved": "i.date_and_time_retrieved", "store_name": "i.store_name", "map_name": "i.map_name",
		"availability": "i.is_available",
	}
	orderByClause, sortBy, order := getSortClause(r, allowedSorts, "price", "DESC")

	var whereConditions []string
	var queryParams []interface{}

	if searchClause, searchParams, err := buildItemSearchClause(searchQuery, "i"); err != nil {
		http.Error(w, "Failed to build item search query", http.StatusInternalServerError)
		return
	} else if searchClause != "" {
		whereConditions = append(whereConditions, searchClause)
		queryParams = append(queryParams, searchParams...)
	}

	if storeNameQuery != "" {
		whereConditions = append(whereConditions, "i.store_name = ?")
		queryParams = append(queryParams, storeNameQuery)
	}
	if selectedType != "" {
		// --- MODIFICATION: Query internal_item_db's 'type' column ---
		// (Duplicating the logic from summaryHandler)
		var dbType string
		switch selectedType {
		case "Healing Item":
			dbType = "Healing"
		case "Usable Item":
			dbType = "Usable"
		case "Miscellaneous":
			dbType = "Etc"
		case "Ammunition":
			dbType = "Ammo"
		case "Card":
			dbType = "Card"
		case "Monster Egg":
			dbType = "PetEgg"
		case "Pet Armor":
			dbType = "PetArmor"
		case "Weapon":
			dbType = "Weapon"
		case "Armor":
			dbType = "Armor"
		case "Cash Shop Item":
			dbType = "Cash"
		default:
			dbType = selectedType
		}
		whereConditions = append(whereConditions, "local_db.type = ?")
		// --- END MODIFICATION ---
		queryParams = append(queryParams, dbType)
	}
	if !showAll {
		whereConditions = append(whereConditions, "i.is_available = 1")
	}

	// --- MODIFICATION: Join internal_item_db ---
	baseQuery := `
		SELECT i.id, i.name_of_the_item, local_db.name_pt, i.item_id, i.quantity, i.price, i.store_name, i.seller_name, i.date_and_time_retrieved, i.map_name, i.map_coordinates, i.is_available
		FROM items i 
		LEFT JOIN internal_item_db local_db ON i.item_id = local_db.item_id
	`
	// --- END MODIFICATION ---

	whereClause := ""
	if len(whereConditions) > 0 {
		whereClause = "WHERE " + strings.Join(whereConditions, " AND ")
	}

	query := fmt.Sprintf(`%s %s %s;`, baseQuery, whereClause, orderByClause)

	rows, err := db.Query(query, queryParams...)
	if err != nil {
		log.Printf("[E] [HTTP] Database query error: %v", err)
		http.Error(w, "Database query failed", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var items []Item
	for rows.Next() {
		var item Item
		var retrievedTime string
		err := rows.Scan(&item.ID, &item.Name, &item.NamePT, &item.ItemID, &item.Quantity, &item.Price, &item.StoreName, &item.SellerName, &retrievedTime, &item.MapName, &item.MapCoordinates, &item.IsAvailable)
		if err != nil {
			log.Printf("[W] [HTTP] Failed to scan row: %v", err)
			continue
		}
		if parsedTime, err := time.Parse(time.RFC3339, retrievedTime); err == nil {
			item.Timestamp = parsedTime.Format("2006-01-02 15:04")
		} else {
			item.Timestamp = retrievedTime
		}
		items = append(items, item)
	}

	data := PageData{
		Items: items, SearchQuery: searchQuery, StoreNameQuery: storeNameQuery, AllStoreNames: allStoreNames,
		SortBy: sortBy, Order: order, ShowAll: showAll, LastScrapeTime: GetLastScrapeTime(),
		VisibleColumns: visibleColumns, AllColumns: allCols, ColumnParams: template.URL(columnParams.Encode()),
		ItemTypes: getItemTypeTabs(), SelectedType: selectedType,
		PageTitle: "Full List",
	}
	renderTemplate(w, r, "full_list.html", data)
}

func activityHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)
		return
	}
	searchQuery := r.FormValue("query")
	soldOnly := r.FormValue("sold_only") == "true"
	const eventsPerPage = 50

	var whereConditions []string
	var params []interface{}

	if searchClause, searchParams, err := buildItemSearchClause(searchQuery, "me"); err != nil {
		http.Error(w, "Failed to build item search query", http.StatusInternalServerError)
		return
	} else if searchClause != "" {
		whereConditions = append(whereConditions, searchClause)
		params = append(params, searchParams...)
	}

	if soldOnly {
		whereConditions = append(whereConditions, "event_type = ?")
		params = append(params, "SOLD")
	}
	whereClause := ""
	if len(whereConditions) > 0 {
		whereClause = "WHERE " + strings.Join(whereConditions, " AND ")
	}

	var totalEvents int
	// --- MODIFICATION: Join internal_item_db ---
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM market_events me LEFT JOIN internal_item_db local_db ON me.item_id = local_db.item_id %s", whereClause)
	// --- END MODIFICATION ---
	if err := db.QueryRow(countQuery, params...).Scan(&totalEvents); err != nil {
		log.Printf("[E] [HTTP] Could not count market events: %v", err)
		http.Error(w, "Could not count market events", http.StatusInternalServerError)
		return
	}

	pagination := newPaginationData(r, totalEvents, eventsPerPage)
	// --- MODIFICATION: Join internal_item_db ---
	query := fmt.Sprintf(`
        SELECT me.event_timestamp, me.event_type, me.item_name, local_db.name_pt, me.item_id, me.details
        FROM market_events me
        LEFT JOIN internal_item_db local_db ON me.item_id = local_db.item_id %s
        ORDER BY me.event_timestamp DESC LIMIT ? OFFSET ?`, whereClause)
	// --- END MODIFICATION ---

	queryArgs := params
	queryArgs = append(queryArgs, eventsPerPage, pagination.Offset)
	// --- END FIX ---
	eventRows, err := db.Query(query, queryArgs...)
	if err != nil {
		// --- THIS IS THE FIX ---
		log.Printf("[E] [HTTP] Could not query for market events: %v", err)
		// --- END FIX ---
		http.Error(w, "Could not query for market events", http.StatusInternalServerError)
		return
	}
	defer eventRows.Close()

	var marketEvents []MarketEvent
	for eventRows.Next() {
		var event MarketEvent
		var detailsStr, timestampStr string
		if err := eventRows.Scan(&timestampStr, &event.EventType, &event.ItemName, &event.NamePT, &event.ItemID, &detailsStr); err != nil {
			log.Printf("[W] [HTTP] Failed to scan market event row: %v", err)
			continue
		}
		if parsedTime, err := time.Parse(time.RFC3339, timestampStr); err == nil {
			event.Timestamp = parsedTime.Format("2006-01-02 15:04")
		} else {
			event.Timestamp = timestampStr
		}
		json.Unmarshal([]byte(detailsStr), &event.Details)
		marketEvents = append(marketEvents, event)
	}

	data := ActivityPageData{
		MarketEvents:   marketEvents,
		LastScrapeTime: GetLastScrapeTime(),
		SearchQuery:    searchQuery,
		SoldOnly:       soldOnly,
		Pagination:     pagination,
		PageTitle:      "Activity",
	}
	renderTemplate(w, r, "activity.html", data)
}

func itemHistoryHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)
		return
	}
	itemName := r.FormValue("name")
	if itemName == "" {
		http.Error(w, "Item name is required", http.StatusBadRequest)
		return
	}
	log.Printf("[D] [HTTP/History] Handling request for item: '%s'", itemName)

	var itemID int
	var itemNamePT sql.NullString
	// --- MODIFICATION: Join internal_item_db ---
	err := db.QueryRow(`
		SELECT i.item_id, local_db.name_pt 
		FROM items i 
		LEFT JOIN internal_item_db local_db ON i.item_id = local_db.item_id
		WHERE i.name_of_the_item = ? AND i.item_id > 0 
		LIMIT 1`, itemName).Scan(&itemID, &itemNamePT)
	// --- END MODIFICATION ---
	if err != nil {
		log.Printf("[D] [HTTP/History] Step 1: Initial item ID/NamePT query failed for '%s': %v", itemName, err)
	} else {
		log.Printf("[D] [HTTP/History] Step 1: Found ItemID: %d, NamePT: '%s'", itemID, itemNamePT.String)
	}

	// --- MODIFICATION: This now calls the new getItemDetailsFromCache (from rms.go)
	// which queries internal_item_db.
	rmsItemDetails := fetchItemDetails(itemID)
	// --- END MODIFICATION ---
	if rmsItemDetails != nil {
		log.Printf("[D] [HTTP/History] Step 2: Successfully fetched details from internal_item_db for ID %d.", itemID)
	} else {
		log.Printf("[D] [HTTP/History] Step 2: No details found in internal_item_db for ID %d.", itemID)
	}

	currentListings, err := fetchCurrentListings(itemName)
	if err != nil {
		log.Printf("[E] [HTTP/History] Current listings query error: %v", err)
		http.Error(w, "Database query for current listings failed", http.StatusInternalServerError)
		return
	}
	log.Printf("[D] [HTTP/History] Step 3: Found %d current (available) listings.", len(currentListings))

	var currentLowest, currentHighest *ItemListing
	if len(currentListings) > 0 {
		currentLowest = &currentListings[0]
		currentHighest = &currentListings[len(currentListings)-1]
		log.Printf("[D] [HTTP/History]     -> Current Lowest: %d z, Current Highest: %d z", currentLowest.Price, currentHighest.Price)
	}

	// --- FIX IS HERE ---
	// Marshal the lowest/highest listings for the script
	// Use "null" as a default JSON value if no item is found
	var currentLowestJSON, currentHighestJSON []byte
	if currentLowest != nil {
		currentLowestJSON, _ = json.Marshal(currentLowest)
	} else {
		currentLowestJSON = []byte("null")
	}

	if currentHighest != nil {
		currentHighestJSON, _ = json.Marshal(currentHighest)
	} else {
		currentHighestJSON = []byte("null")
	}
	// --- END FIX ---

	finalPriceHistory, err := fetchPriceHistory(itemName)
	if err != nil {
		log.Printf("[E] [HTTP/History] History change query error: %v", err)
		http.Error(w, "Database query for changes failed", http.StatusInternalServerError)
		return
	}
	log.Printf("[D] [HTTP/History] Step 4: Found %d unique price points for history graph.", len(finalPriceHistory))
	priceHistoryJSON, _ := json.Marshal(finalPriceHistory)

	var overallLowest, overallHighest sql.NullInt64
	db.QueryRow(`
        SELECT MIN(CAST(REPLACE(REPLACE(price, ',', ''), 'z', '') AS INTEGER)), 
               MAX(CAST(REPLACE(REPLACE(price, ',', ''), 'z', '') AS INTEGER))
        FROM items WHERE name_of_the_item = ?;
    `, itemName).Scan(&overallLowest, &overallHighest)
	log.Printf("[D] [HTTP/History] Step 5: Found Overall Lowest: %d z, Overall Highest: %d z", overallLowest.Int64, overallHighest.Int64)

	const listingsPerPage = 50

	// --- FIX: Get total count FIRST ---
	var totalListings int
	err = db.QueryRow("SELECT COUNT(*) FROM items WHERE name_of_the_item = ?", itemName).Scan(&totalListings)
	if err != nil {
		log.Printf("[E] [HTTP/History] Failed to count all listings: %v", err)
		http.Error(w, "Database query for all listings failed", http.StatusInternalServerError)
		return
	}
	log.Printf("[D] [HTTP/History] Step 6a: Found %d total historical listings.", totalListings)

	// --- FIX: Create pagination ONCE with the correct total ---
	pagination := newPaginationData(r, totalListings, listingsPerPage)

	// --- FIX: Pass pagination to the fetcher ---
	allListings, err := fetchAllListings(itemName, pagination, listingsPerPage)
	if err != nil {
		log.Printf("[E] [HTTP/History] All listings query error: %v", err)
		http.Error(w, "Database query for all listings failed", http.StatusInternalServerError)
		return
	}
	log.Printf("[D] [HTTP/History] Step 6b: Returning %d listings for this page.", len(allListings))
	// --- END FIX ---

	// --- FIX: Create filter string for pagination ---
	filterString := "&name=" + url.QueryEscape(itemName)
	// --- END FIX ---

	data := HistoryPageData{
		ItemName:           itemName,
		ItemNamePT:         itemNamePT,
		PriceDataJSON:      template.JS(priceHistoryJSON),
		CurrentLowestJSON:  template.JS(currentLowestJSON),
		CurrentHighestJSON: template.JS(currentHighestJSON),
		OverallLowest:      overallLowest.Int64,
		OverallHighest:     overallHighest.Int64,
		CurrentLowest:      currentLowest,
		CurrentHighest:     currentHighest,
		ItemDetails:        rmsItemDetails,
		AllListings:        allListings,
		LastScrapeTime:     GetLastScrapeTime(),
		TotalListings:      totalListings,
		Pagination:         pagination,
		PageTitle:          itemName,
		Filter:             template.URL(filterString), // <-- ADDED
	}

	log.Printf("[D] [HTTP/History] Rendering template for '%s' with all data.", itemName)
	renderTemplate(w, r, "history.html", data)
}

// I am also including the helper functions that were extracted in the refactor,
// as they are called by the main handler.

// fetchItemDetails attempts to get RMSItem details from the internal item DB.
func fetchItemDetails(itemID int) *RMSItem {
	if itemID <= 0 {
		return nil
	}

	// --- MODIFICATION: Only query the internal_item_db. ---
	// Fallback scraping and saving logic has been removed.
	cachedItem, err := getItemDetailsFromCache(itemID)
	if err == nil {
		log.Printf("[D] [ItemDB] Found item %d in internal_item_db.", itemID)
		return cachedItem // Found in local DB
	}

	log.Printf("[D] [ItemDB] Item %d not found in internal_item_db: %v", itemID, err)
	return nil // Not found
	// --- END MODIFICATION ---
}

// fetchCurrentListings gets all currently available listings for an item.
func fetchCurrentListings(itemName string) ([]ItemListing, error) {
	query := `
		SELECT CAST(REPLACE(REPLACE(price, ',', ''), 'z', '') AS INTEGER) as price_int, 
		       quantity, store_name, seller_name, map_name, map_coordinates, date_and_time_retrieved
		FROM items WHERE name_of_the_item = ? AND is_available = 1 
		ORDER BY price_int ASC;
	`
	rows, err := db.Query(query, itemName)
	if err != nil {
		return nil, fmt.Errorf("current listings query error: %w", err)
	}
	defer rows.Close()

	var listings []ItemListing
	for rows.Next() {
		var listing ItemListing
		var timestampStr string
		if err := rows.Scan(&listing.Price, &listing.Quantity, &listing.StoreName, &listing.SellerName, &listing.MapName, &listing.MapCoordinates, &timestampStr); err != nil {
			log.Printf("[W] [HTTP/History] Failed to scan current listing row: %v", err)
			continue
		}
		if parsedTime, err := time.Parse(time.RFC3339, timestampStr); err == nil {
			listing.Timestamp = parsedTime.Format("2006-01-02 15:04")
		} else {
			listing.Timestamp = timestampStr
		}
		listings = append(listings, listing)
	}
	return listings, nil
}

// fetchPriceHistory aggregates the lowest/highest price points over time for the graph.
func fetchPriceHistory(itemName string) ([]PricePointDetails, error) {
	priceChangeQuery := `
		SELECT
			t_lowest.date_and_time_retrieved,
			t_lowest.price_int,
			t_lowest.quantity,
			t_lowest.store_name,
			t_lowest.seller_name,
			t_lowest.map_name,
			t_lowest.map_coordinates,
			t_highest.price_int,
			t_highest.quantity,
			t_highest.store_name,
			t_highest.seller_name,
			t_highest.map_name,
			t_highest.map_coordinates
		FROM
			(
				-- Subquery to find the row with the lowest price for each timestamp
				SELECT 
					i1.date_and_time_retrieved,
					CAST(REPLACE(REPLACE(i1.price, ',', ''), 'z', '') AS INTEGER) as price_int,
					i1.quantity,
					i1.store_name,
					i1.seller_name,
					i1.map_name,
					i1.map_coordinates
				FROM items i1
				WHERE i1.name_of_the_item = ?
				AND i1.id = (
					SELECT i_min.id
					FROM items i_min
					WHERE i_min.name_of_the_item = i1.name_of_the_item
					  AND i_min.date_and_time_retrieved = i1.date_and_time_retrieved
					ORDER BY CAST(REPLACE(REPLACE(i_min.price, ',', ''), 'z', '') AS INTEGER) ASC, i_min.id DESC
					LIMIT 1
				)
			) AS t_lowest
		JOIN
			(
				-- Subquery to find the row with the highest price for each timestamp
				SELECT 
					i2.date_and_time_retrieved,
					CAST(REPLACE(REPLACE(i2.price, ',', ''), 'z', '') AS INTEGER) as price_int,
					i2.quantity,
					i2.store_name,
					i2.seller_name,
					i2.map_name,
					i2.map_coordinates
				FROM items i2
				WHERE i2.name_of_the_item = ?
				AND i2.id = (
					SELECT i_max.id
					FROM items i_max
					WHERE i_max.name_of_the_item = i2.name_of_the_item
					  AND i_max.date_and_time_retrieved = i2.date_and_time_retrieved
					ORDER BY CAST(REPLACE(REPLACE(i_max.price, ',', ''), 'z', '') AS INTEGER) DESC, i_max.id DESC
					LIMIT 1
				)
			) AS t_highest 
		ON t_lowest.date_and_time_retrieved = t_highest.date_and_time_retrieved
		ORDER BY t_lowest.date_and_time_retrieved ASC;
    `
	rows, err := db.Query(priceChangeQuery, itemName, itemName)
	if err != nil {
		return nil, fmt.Errorf("history change query error: %w", err)
	}
	defer rows.Close()

	var finalPriceHistory []PricePointDetails
	for rows.Next() {
		var p PricePointDetails
		var timestampStr string
		err := rows.Scan(&timestampStr, &p.LowestPrice, &p.LowestQuantity, &p.LowestStoreName, &p.LowestSellerName, &p.LowestMapName, &p.LowestMapCoords,
			&p.HighestPrice, &p.HighestQuantity, &p.HighestStoreName, &p.HighestSellerName, &p.HighestMapName, &p.HighestMapCoords)
		if err != nil {
			log.Printf("[W] [HTTP/History] Failed to scan history row: %v", err)
			continue
		}

		t, _ := time.Parse(time.RFC3339, timestampStr)
		p.Timestamp = t.Format("2006-01-02 15:04")

		// This logic de-duplicates consecutive identical price points
		if len(finalPriceHistory) == 0 ||
			finalPriceHistory[len(finalPriceHistory)-1].LowestPrice != p.LowestPrice ||
			finalPriceHistory[len(finalPriceHistory)-1].HighestPrice != p.HighestPrice {
			finalPriceHistory = append(finalPriceHistory, p)
		}
	}
	return finalPriceHistory, nil
}

// fetchAllListings retrieves a paginated list of all historical listings for an item.
func fetchAllListings(itemName string, pagination PaginationData, listingsPerPage int) ([]Item, error) {
	// --- FIX: Total count is now done in the handler ---
	query := `
		SELECT i.id, i.name_of_the_item, local_db.name_pt, i.item_id, i.quantity, i.price, 
		       i.store_name, i.seller_name, i.date_and_time_retrieved, i.map_name, 
			   i.map_coordinates, i.is_available
		FROM items i 
		LEFT JOIN internal_item_db local_db ON i.item_id = local_db.item_id 
		WHERE i.name_of_the_item = ? 
		ORDER BY i.is_available DESC, i.date_and_time_retrieved DESC 
		LIMIT ? OFFSET ?;
	`
	rows, err := db.Query(query, itemName, listingsPerPage, pagination.Offset)
	if err != nil {
		return nil, fmt.Errorf("all listings query error: %w", err) // <-- FIX: Return nil, error
	}
	defer rows.Close()

	var allListings []Item
	for rows.Next() {
		var listing Item
		var timestampStr string
		if err := rows.Scan(&listing.ID, &listing.Name, &listing.NamePT, &listing.ItemID, &listing.Quantity, &listing.Price, &listing.StoreName, &listing.SellerName, &timestampStr, &listing.MapName, &listing.MapCoordinates, &listing.IsAvailable); err != nil {
			log.Printf("[W] [HTTP/History] Failed to scan all listing row: %v", err)
			continue
		}
		if parsedTime, err := time.Parse(time.RFC3339, timestampStr); err == nil {
			listing.Timestamp = parsedTime.Format("2006-01-02 15:04")
		} else {
			listing.Timestamp = timestampStr
		}
		allListings = append(allListings, listing)
	}
	return allListings, nil // <-- FIX: Return listings, nil
}

func playerCountHandler(w http.ResponseWriter, r *http.Request) {
	interval := r.URL.Query().Get("interval")
	if interval == "" {
		interval = "7d"
	}

	now := time.Now()
	var viewStart time.Time

	switch interval {
	case "6h":
		viewStart = now.Add(-6 * time.Hour)
	case "24h":
		viewStart = now.Add(-24 * time.Hour)
	case "30d":
		viewStart = now.Add(-30 * 24 * time.Hour)
	case "7d":
		fallthrough
	default:
		interval = "7d"
		viewStart = now.Add(-7 * 24 * time.Hour)
	}

	whereClause := "WHERE timestamp >= ?"
	params := []interface{}{viewStart.Format(time.RFC3339)}
	const maxGraphDataPoints = 720
	var query string
	duration := now.Sub(viewStart)

	if duration.Minutes() > maxGraphDataPoints {
		bucketSizeInSeconds := int(duration.Seconds()) / maxGraphDataPoints
		if bucketSizeInSeconds < 60 {
			bucketSizeInSeconds = 60
		}
		log.Printf("[D] [HTTP/Player] Player graph: Downsampling data for '%s' interval. Bucket size: %d seconds.", interval, bucketSizeInSeconds)
		query = fmt.Sprintf(`
			SELECT MIN(timestamp), CAST(AVG(count) AS INTEGER), CAST(AVG(seller_count) AS INTEGER)
			FROM player_history %s GROUP BY CAST(unixepoch(timestamp) / %d AS INTEGER) ORDER BY 1 ASC`, whereClause, bucketSizeInSeconds)
	} else {
		log.Printf("[D] [HTTP/Player] Player graph: Fetching all data points for '%s' interval.", interval)
		query = fmt.Sprintf("SELECT timestamp, count, seller_count FROM player_history %s ORDER BY timestamp ASC", whereClause)
	}

	rows, err := db.Query(query, params...)
	if err != nil {
		log.Printf("[E] [HTTP/Player] Could not query for player history: %v", err)
		http.Error(w, "Could not query for player history", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var playerHistory []PlayerCountPoint
	activeDatesWithData := make(map[string]struct{})

	// --- NEW: Variables for interval stats ---
	var maxActiveInterval int = -1
	var minActiveInterval int = -1
	var totalActiveInterval int64 = 0
	var maxActiveIntervalTime string = "N/A"
	var dataPointCount int64 = 0
	// --- END NEW ---

	for rows.Next() {
		var point PlayerCountPoint
		var timestampStr string
		var sellerCount sql.NullInt64
		if err := rows.Scan(&timestampStr, &point.Count, &sellerCount); err != nil {
			log.Printf("[W] [HTTP/Player] Failed to scan player history row: %v", err)
			continue
		}
		point.SellerCount = int(sellerCount.Int64)

		// --- MODIFICATION: Clamp Delta to 0 ---
		point.Delta = point.Count - point.SellerCount
		if point.Delta < 0 {
			point.Delta = 0
		}
		// --- END MODIFICATION ---

		if parsedTime, err := time.Parse(time.RFC3339, timestampStr); err == nil {
			point.Timestamp = parsedTime.Format("2006-01-02 15:04")
			activeDatesWithData[parsedTime.Format("2006-01-02")] = struct{}{}
		} else {
			point.Timestamp = timestampStr
		}
		playerHistory = append(playerHistory, point)

		// --- NEW: Calculate interval stats ---
		activePlayers := point.Delta // This now uses the clamped (>= 0) delta
		if maxActiveInterval == -1 || activePlayers > maxActiveInterval {
			maxActiveInterval = activePlayers
			maxActiveIntervalTime = point.Timestamp
		}
		if minActiveInterval == -1 || activePlayers < minActiveInterval {
			minActiveInterval = activePlayers
		}
		totalActiveInterval += int64(activePlayers)
		dataPointCount++
		// --- END NEW ---
	}
	playerHistoryJSON, _ := json.Marshal(playerHistory)
	eventIntervals := generateEventIntervals(viewStart, now, definedEvents, activeDatesWithData)
	eventIntervalsJSON, _ := json.Marshal(eventIntervals)

	var latestCount, latestSellerCount int
	db.QueryRow("SELECT count, seller_count FROM player_history ORDER BY timestamp DESC LIMIT 1").Scan(&latestCount, &latestSellerCount)

	// --- MODIFICATION: Clamp Latest Active Players to 0 ---
	latestActivePlayers := latestCount - latestSellerCount
	if latestActivePlayers < 0 {
		latestActivePlayers = 0
	}
	// --- END MODIFICATION ---

	var historicalMaxActive int
	var historicalMaxTimestampStr sql.NullString
	db.QueryRow("SELECT (count - COALESCE(seller_count, 0)), timestamp FROM player_history ORDER BY 1 DESC LIMIT 1").Scan(&historicalMaxActive, &historicalMaxTimestampStr)

	// --- MODIFICATION: Clamp Historical Max to 0 ---
	if historicalMaxActive < 0 {
		historicalMaxActive = 0
	}
	// --- END MODIFICATION ---

	historicalMaxTime := "N/A"
	if historicalMaxTimestampStr.Valid {
		if parsedTime, err := time.Parse(time.RFC3339, historicalMaxTimestampStr.String); err == nil {
			historicalMaxTime = parsedTime.Format("2006-01-02 15:04")
		}
	}

	// --- NEW: Finalize interval stats ---
	var avgActiveInterval int
	if dataPointCount > 0 {
		avgActiveInterval = int(totalActiveInterval / dataPointCount)
	}
	if maxActiveInterval == -1 {
		maxActiveInterval = 0
	}
	if minActiveInterval == -1 {
		minActiveInterval = 0
	}
	// --- END NEW ---

	data := PlayerCountPageData{
		PlayerDataJSON:                 template.JS(playerHistoryJSON),
		LastScrapeTime:                 GetLastScrapeTime(),
		SelectedInterval:               interval,
		EventDataJSON:                  template.JS(eventIntervalsJSON),
		LatestActivePlayers:            latestActivePlayers,
		HistoricalMaxActivePlayers:     historicalMaxActive,
		HistoricalMaxActivePlayersTime: historicalMaxTime,
		PageTitle:                      "Player Count",

		// --- NEW: Pass data to template ---
		IntervalPeakActive:     maxActiveInterval,
		IntervalPeakActiveTime: maxActiveIntervalTime,
		IntervalAvgActive:      avgActiveInterval,
		IntervalLowActive:      minActiveInterval,
		// --- END NEW ---
	}
	renderTemplate(w, r, "players.html", data)
}

func characterHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)
		return
	}
	searchName := r.FormValue("name_query")
	selectedClass := r.FormValue("class_filter")
	selectedGuild := r.FormValue("guild_filter")
	selectedCols := r.Form["cols"]
	graphFilter := r.Form["graph_filter"]

	isInitialLoad := len(r.Form) == 0
	const playersPerPage = 50

	specialPlayers := map[string]bool{
		"Purity Ring": true, "Bafo MvP": true, "franco bs": true, "franco alchie": true, "Afanei": true,
		"GiupSankino": true, "MacroBot1000": true, "Sileeent": true, "Shiiv": true, "Majim Lipe": true,
		"Solidao": true, "WildTig3r": true, "No Glory": true,
	}
	guildMasters := make(map[string]bool)
	masterRows, err := db.Query("SELECT DISTINCT master FROM guilds WHERE master IS NOT NULL AND master != ''")
	if err == nil {
		defer masterRows.Close()
		for masterRows.Next() {
			var masterName string
			if err := masterRows.Scan(&masterName); err == nil {
				guildMasters[masterName] = true
			}
		}
	}

	allCols := []Column{
		{ID: "rank", DisplayName: "Rank"}, {ID: "base_level", DisplayName: "Base Lvl"}, {ID: "job_level", DisplayName: "Job Lvl"},
		{ID: "experience", DisplayName: "Exp %"}, {ID: "zeny", DisplayName: "Zeny"}, {ID: "class", DisplayName: "Class"},
		{ID: "guild", DisplayName: "Guild"}, {ID: "last_updated", DisplayName: "Last Updated"}, {ID: "last_active", DisplayName: "Last Active"},
	}
	visibleColumns := make(map[string]bool)
	columnParams := url.Values{}      // This is kept for the form, but not for pagination filter
	graphFilterParams := url.Values{} // This is kept for the form, but not for pagination filter

	if isInitialLoad {
		visibleColumns["base_level"], visibleColumns["job_level"], visibleColumns["experience"] = true, true, true
		visibleColumns["class"], visibleColumns["guild"], visibleColumns["last_active"] = true, true, true

		// --- FIX: Populate selectedCols with the defaults ---
		selectedCols = []string{} // Initialize the slice
		for colID := range visibleColumns {
			columnParams.Add("cols", colID)            // Still needed for header links
			selectedCols = append(selectedCols, colID) // Add default col to the slice
		}
		// --- END FIX ---

		graphFilter = []string{"second"}
	} else {
		for _, col := range selectedCols {
			visibleColumns[col] = true
			columnParams.Add("cols", col) // Still needed for header links
		}
	}
	for _, f := range graphFilter {
		graphFilterParams.Add("graph_filter", f) // Still needed for header links
	}

	var allClasses []string
	classRows, err := db.Query("SELECT DISTINCT class FROM characters ORDER BY class ASC")
	if err == nil {
		defer classRows.Close()
		for classRows.Next() {
			var className string
			if err := classRows.Scan(&className); err == nil {
				allClasses = append(allClasses, className)
			}
		}
	}

	var whereConditions []string
	var params []interface{}
	if searchName != "" {
		whereConditions = append(whereConditions, "name LIKE ?")
		params = append(params, "%"+searchName+"%")
	}
	if selectedClass != "" {
		whereConditions = append(whereConditions, "class = ?")
		params = append(params, selectedClass)
	}
	if selectedGuild != "" {
		whereConditions = append(whereConditions, "guild_name = ?")
		params = append(params, selectedGuild)
	}
	whereClause := ""
	if len(whereConditions) > 0 {
		whereClause = "WHERE " + strings.Join(whereConditions, " AND ")
	}

	classDistribution := make(map[string]int)
	distQuery := fmt.Sprintf("SELECT class, COUNT(*) FROM characters %s GROUP BY class", whereClause)
	distRows, err := db.Query(distQuery, params...)
	if err == nil {
		defer distRows.Close()
		for distRows.Next() {
			var className string
			var count int
			if err := distRows.Scan(&className, &count); err == nil {
				classDistribution[className] = count
			}
		}
	}

	noviceClasses := map[string]bool{"Aprendiz": true, "Super Aprendiz": true}
	firstClasses := map[string]bool{"Arqueiro": true, "Espadachim": true, "Gatuno": true, "Mago": true, "Mercador": true, "Noviço": true}
	secondClasses := map[string]bool{"Alquimista": true, "Arruaceiro": true, "Bardo": true, "Bruxo": true, "Cavaleiro": true, "Caçador": true, "Ferreiro": true, "Mercenário": true, "Monge": true, "Odalisca": true, "Sacerdote": true, "Sábio": true, "Templário": true}
	graphFilterMap := make(map[string]bool)
	for _, f := range graphFilter {
		graphFilterMap[f] = true
	}

	chartData := make(map[string]int)
	for class, count := range classDistribution {
		if noviceClasses[class] && graphFilterMap["novice"] {
			chartData[class] = count
		} else if firstClasses[class] && graphFilterMap["first"] {
			chartData[class] = count
		} else if secondClasses[class] && graphFilterMap["second"] {
			chartData[class] = count
		}
	}
	classDistJSON, _ := json.Marshal(chartData)

	allowedSorts := map[string]string{
		"rank": "rank", "name": "name", "base_level": "base_level", "job_level": "job_level", "experience": "experience",
		"zeny": "zeny", "class": "class", "guild": "guild_name", "last_updated": "last_updated", "last_active": "last_active",
	}
	orderByClause, sortBy, order := getSortClause(r, allowedSorts, "rank", "ASC")

	var totalPlayers int
	var totalZeny sql.NullInt64
	countQuery := fmt.Sprintf("SELECT COUNT(*), SUM(zeny) FROM characters %s", whereClause)
	if err := db.QueryRow(countQuery, params...).Scan(&totalPlayers, &totalZeny); err != nil {
		http.Error(w, "Could not count player characters", http.StatusInternalServerError)
		return
	}

	pagination := newPaginationData(r, totalPlayers, playersPerPage)
	query := fmt.Sprintf(`SELECT rank, name, base_level, job_level, experience, class, guild_name, zeny, last_updated, last_active
		FROM characters %s %s LIMIT ? OFFSET ?`, whereClause, orderByClause)

	queryArgs := params
	queryArgs = append(queryArgs, playersPerPage, pagination.Offset)

	rows, err := db.Query(query, queryArgs...)
	if err != nil {
		http.Error(w, "Could not query for player characters", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var players []PlayerCharacter
	for rows.Next() {
		var p PlayerCharacter
		var lastUpdatedStr, lastActiveStr string
		if err := rows.Scan(&p.Rank, &p.Name, &p.BaseLevel, &p.JobLevel, &p.Experience, &p.Class, &p.GuildName, &p.Zeny, &lastUpdatedStr, &lastActiveStr); err != nil {
			log.Printf("[W] [HTTP/Char] Failed to scan player character row: %v", err)
			continue
		}
		if t, err := time.Parse(time.RFC3339, lastUpdatedStr); err == nil {
			p.LastUpdated = t.Format("2006-01-02 15:04")
		}
		if t, err := time.Parse(time.RFC3339, lastActiveStr); err == nil {
			p.LastActive = t.Format("2006-01-02 15:04")
		}
		p.IsActive = (lastUpdatedStr == lastActiveStr) && lastUpdatedStr != ""
		p.IsGuildLeader = guildMasters[p.Name]
		p.IsSpecial = specialPlayers[p.Name]
		players = append(players, p)
	}

	// --- ADD THIS BLOCK to build the filter string ---
	filterValues := url.Values{}
	if searchName != "" {
		filterValues.Set("name_query", searchName)
	}
	if selectedClass != "" {
		filterValues.Set("class_filter", selectedClass)
	}
	if selectedGuild != "" {
		filterValues.Set("guild_filter", selectedGuild)
	}
	// 'selectedCols' is the []string of column names
	for _, col := range selectedCols {
		filterValues.Add("cols", col)
	}
	// 'graphFilter' is the []string of graph filters
	for _, f := range graphFilter {
		filterValues.Add("graph_filter", f)
	}
	// Add sort parameters
	// filterValues.Set("sort_by", sortBy)
	// filterValues.Set("order", order)

	var filterString string
	if encodedFilter := filterValues.Encode(); encodedFilter != "" {
		filterString = "&" + encodedFilter
	}
	// --- END OF NEW BLOCK ---

	// --- NOW, UPDATE the data block: ---
	data := CharacterPageData{
		Players: players, LastScrapeTime: GetLastScrapeTime(),
		SearchQuery:   searchName,
		SelectedClass: selectedClass, SelectedGuild: selectedGuild, AllClasses: allClasses, SortBy: sortBy, Order: order,
		VisibleColumns: visibleColumns, AllColumns: allCols,
		// ColumnParams: template.URL(columnParams.Encode()), // <-- REMOVED
		// GraphFilterParams: template.URL(graphFilterParams.Encode()), // <-- REMOVED
		Filter:     template.URL(filterString), // <-- ADDED
		Pagination: pagination, TotalPlayers: totalPlayers, TotalZeny: totalZeny.Int64,
		ClassDistributionJSON: template.JS(classDistJSON), GraphFilter: graphFilterMap,
		HasChartData: len(chartData) > 1,
		PageTitle:    "Characters",
	}
	renderTemplate(w, r, "characters.html", data)
}

func guildHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)
		return
	}
	searchName := r.FormValue("name_query")
	const guildsPerPage = 50

	var whereConditions []string
	var params []interface{}
	if searchName != "" {
		whereConditions = append(whereConditions, "g.name LIKE ?")
		params = append(params, "%"+searchName+"%")
	}
	whereClause := ""
	if len(whereConditions) > 0 {
		whereClause = "WHERE " + strings.Join(whereConditions, " AND ")
	}

	allowedSorts := map[string]string{
		"rank": "rank", "name": "g.name", "level": "g.level", "master": "g.master",
		"members": "member_count", "zeny": "total_zeny", "avg_level": "avg_base_level",
	}
	orderByClause, sortBy, order := getSortClause(r, allowedSorts, "level", "DESC")

	var totalGuilds int
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM guilds g %s", whereClause)
	if err := db.QueryRow(countQuery, params...).Scan(&totalGuilds); err != nil {
		http.Error(w, "Could not count guilds", http.StatusInternalServerError)
		return
	}

	pagination := newPaginationData(r, totalGuilds, guildsPerPage)
	query := fmt.Sprintf(`
		SELECT g.name, g.level, g.experience, g.master, g.emblem_url,
			COALESCE(cs.member_count, 0) as member_count,
			COALESCE(cs.total_zeny, 0) as total_zeny,
			COALESCE(cs.avg_base_level, 0) as avg_base_level
		FROM guilds g
		LEFT JOIN (
			SELECT guild_name, COUNT(*) as member_count, SUM(zeny) as total_zeny, AVG(base_level) as avg_base_level
			FROM characters
			WHERE guild_name IS NOT NULL AND guild_name != ''
			GROUP BY guild_name
		) cs ON g.name = cs.guild_name
		%s %s LIMIT ? OFFSET ?`, whereClause, orderByClause)
	finalParams := append(params, guildsPerPage, pagination.Offset)

	rows, err := db.Query(query, finalParams...)
	if err != nil {
		log.Printf("[E] [HTTP/Guild] Could not query for guilds: %v", err)
		http.Error(w, "Could not query for guilds", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var guilds []Guild
	for rows.Next() {
		var g Guild
		if err := rows.Scan(&g.Name, &g.Level, &g.Experience, &g.Master, &g.EmblemURL, &g.MemberCount, &g.TotalZeny, &g.AvgBaseLevel); err != nil {
			log.Printf("[W] [HTTP/Guild] Failed to scan guild row: %v", err)
			continue
		}
		guilds = append(guilds, g)
	}

	data := GuildPageData{
		Guilds:              guilds,
		LastGuildUpdateTime: GetLastScrapeTime(),
		SearchName:          searchName,
		SortBy:              sortBy,
		Order:               order,
		Pagination:          pagination,
		TotalGuilds:         totalGuilds,
		PageTitle:           "Guilds",
	}
	renderTemplate(w, r, "guilds.html", data)
}

func mvpKillsHandler(w http.ResponseWriter, r *http.Request) {
	headers := []MvpHeader{{MobID: "total", MobName: "Total Kills"}}
	for _, mobID := range mvpMobIDs {
		headers = append(headers, MvpHeader{MobID: mobID, MobName: mvpNames[mobID]})
	}

	allowedSorts := map[string]string{"name": "character_name"}
	var sumParts []string
	for _, mobID := range mvpMobIDs {
		colName := fmt.Sprintf("mvp_%s", mobID)
		allowedSorts[mobID] = colName
		sumParts = append(sumParts, colName)
	}
	allowedSorts["total"] = fmt.Sprintf("(%s)", strings.Join(sumParts, " + "))

	orderByClause, sortBy, order := getSortClause(r, allowedSorts, "total", "DESC")

	query := fmt.Sprintf("SELECT * FROM character_mvp_kills %s", orderByClause)
	rows, err := db.Query(query)
	if err != nil {
		log.Printf("[E] [HTTP/MVP] Could not query for MVP kills: %v", err)
		http.Error(w, "Could not query MVP kills", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	cols, _ := rows.Columns()
	var players []MvpKillEntry
	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			log.Printf("[W] [HTTP/MVP] Failed to scan MVP kill row: %v", err)
			continue
		}
		player := MvpKillEntry{Kills: make(map[string]int)}
		totalKills := 0
		for i, colName := range cols {
			val := columns[i]
			if colName == "character_name" {
				player.CharacterName = val.(string)
			} else if strings.HasPrefix(colName, "mvp_") {
				mobID := strings.TrimPrefix(colName, "mvp_")
				displayKillCount := int(val.(int64)) - MvpKillCountOffset
				if displayKillCount < 0 {
					displayKillCount = 0
				}
				player.Kills[mobID] = displayKillCount
				totalKills += displayKillCount
			}
		}
		player.TotalKills = totalKills
		players = append(players, player)
	}

	data := MvpKillPageData{
		Players: players, Headers: headers, SortBy: sortBy, Order: order,
		LastScrapeTime: GetLastScrapeTime(),
		PageTitle:      "MVP Kills",
	}
	renderTemplate(w, r, "mvp_kills.html", data)
}

func characterDetailHandler(w http.ResponseWriter, r *http.Request) {
	charName := r.URL.Query().Get("name")
	if charName == "" {
		http.Error(w, "Character name is required", http.StatusBadRequest)
		return
	}

	classImages := map[string]string{
		"Aprendiz": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/8/8b/Icon_jobs_0.png", "Super Aprendiz": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/c/c7/Icon_jobs_4001.png",
		"Arqueiro": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/9/97/Icon_jobs_3.png", "Espadachim": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/5/5b/Icon_jobs_1.png",
		"Gatuno": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/3/3c/Icon_jobs_6.png", "Mago": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/9/99/Icon_jobs_2.png",
		"Mercador": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/9/9e/Icon_jobs_5.png", "Noviço": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/c/c5/Icon_jobs_4.png",
		"Alquimista": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/c/c7/Icon_jobs_18.png", "Arruaceiro": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/4/48/Icon_jobs_17.png",
		"Bardo": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/6/69/Icon_jobs_19.png", "Bruxo": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/0/09/Icon_jobs_9.png",
		"Cavaleiro": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/1/1d/Icon_jobs_7.png", "Caçador": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/e/eb/Icon_jobs_11.png",
		"Ferreiro": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/7/7b/Icon_jobs_10.png", "Mercenário": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/9/9c/Icon_jobs_12.png",
		"Monge": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/4/44/Icon_jobs_15.png", "Odalisca": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/d/dc/Icon_jobs_20.png",
		"Sacerdote": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/3/3a/Icon_jobs_8.png", "Sábio": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/0/0e/Icon_jobs_16.png",
		"Templário": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/e/e1/Icon_jobs_14.png",
	}

	var p PlayerCharacter
	var lastUpdatedStr, lastActiveStr string
	query := `SELECT rank, name, base_level, job_level, experience, class, guild_name, zeny, last_updated, last_active FROM characters WHERE name = ?`
	err := db.QueryRow(query, charName).Scan(&p.Rank, &p.Name, &p.BaseLevel, &p.JobLevel, &p.Experience, &p.Class, &p.GuildName, &p.Zeny, &lastUpdatedStr, &lastActiveStr)
	if err != nil {
		if err == sql.ErrNoRows {
			http.Error(w, "Character not found", http.StatusNotFound)
		} else {
			log.Printf("[E] [HTTP/Char] Could not query for character '%s': %v", charName, err)
			http.Error(w, "Database query for character failed", http.StatusInternalServerError)
		}
		return
	}

	if t, err := time.Parse(time.RFC3339, lastUpdatedStr); err == nil {
		p.LastUpdated = t.Format("2006-01-02 15:04")
	}
	if t, err := time.Parse(time.RFC3339, lastActiveStr); err == nil {
		p.LastActive = t.Format("2006-01-02 15:04")
	}
	p.IsActive = (lastUpdatedStr == lastActiveStr) && lastUpdatedStr != ""

	var guild *Guild
	if p.GuildName.Valid {
		g := Guild{}
		guildQuery := `SELECT name, level, master, (SELECT COUNT(*) FROM characters WHERE guild_name = guilds.name) FROM guilds WHERE name = ?`
		if err := db.QueryRow(guildQuery, p.GuildName.String).Scan(&g.Name, &g.Level, &g.Master, &g.MemberCount); err == nil {
			guild = &g
			p.IsGuildLeader = (p.Name == g.Master)
		}
	}

	mvpKills := MvpKillEntry{CharacterName: p.Name, Kills: make(map[string]int)}
	var mvpCols []string
	for _, mobID := range mvpMobIDs {
		mvpCols = append(mvpCols, fmt.Sprintf("mvp_%s", mobID))
	}
	mvpQuery := fmt.Sprintf("SELECT %s FROM character_mvp_kills WHERE character_name = ?", strings.Join(mvpCols, ", "))
	scanDest := make([]interface{}, len(mvpMobIDs))
	for i := range mvpMobIDs {
		scanDest[i] = new(int)
	}

	if err := db.QueryRow(mvpQuery, charName).Scan(scanDest...); err == nil {
		totalKills := 0
		for i, mobID := range mvpMobIDs {
			killCount := *scanDest[i].(*int)
			mvpKills.Kills[mobID] = killCount
			totalKills += killCount
		}
		mvpKills.TotalKills = totalKills
	}

	var mvpHeaders []MvpHeader
	for _, mobID := range mvpMobIDs {
		if name, ok := mvpNames[mobID]; ok {
			mvpHeaders = append(mvpHeaders, MvpHeader{MobID: mobID, MobName: name})
		}
	}

	var guildHistory []CharacterChangelog
	guildHistoryQuery := `SELECT change_time, activity_description FROM character_changelog
		WHERE character_name = ? AND (activity_description LIKE '%joined guild%' OR activity_description LIKE '%left guild%')
		ORDER BY change_time DESC`
	guildHistoryRows, err := db.Query(guildHistoryQuery, charName)
	if err == nil {
		defer guildHistoryRows.Close()
		for guildHistoryRows.Next() {
			var entry CharacterChangelog
			var timestampStr string
			if err := guildHistoryRows.Scan(&timestampStr, &entry.ActivityDescription); err == nil {
				if t, err := time.Parse(time.RFC3339, timestampStr); err == nil {
					entry.ChangeTime = t.Format("2006-01-02")
				}
				guildHistory = append(guildHistory, entry)
			}
		}
	}

	const entriesPerPage = 25
	var totalChangelogEntries int
	db.QueryRow("SELECT COUNT(*) FROM character_changelog WHERE character_name = ?", charName).Scan(&totalChangelogEntries)

	pagination := newPaginationData(r, totalChangelogEntries, entriesPerPage)
	changelogQuery := `SELECT change_time, activity_description FROM character_changelog WHERE character_name = ? ORDER BY change_time DESC LIMIT ? OFFSET ?`
	changelogRows, err := db.Query(changelogQuery, charName, entriesPerPage, pagination.Offset)
	if err != nil {
		http.Error(w, "Could not query for character changelog", http.StatusInternalServerError)
		return
	}
	defer changelogRows.Close()

	var changelogEntries []CharacterChangelog
	for changelogRows.Next() {
		var entry CharacterChangelog
		var timestampStr string
		if err := changelogRows.Scan(&timestampStr, &entry.ActivityDescription); err == nil {
			if t, err := time.Parse(time.RFC3339, timestampStr); err == nil {
				entry.ChangeTime = t.Format("2006-01-02 15:04:05")
			}
			changelogEntries = append(changelogEntries, entry)
		}
	}

	data := CharacterDetailPageData{
		Character:           p,
		Guild:               guild,
		MvpKills:            mvpKills,
		MvpHeaders:          mvpHeaders,
		GuildHistory:        guildHistory,
		LastScrapeTime:      GetLastScrapeTime(),
		ClassImageURL:       classImages[p.Class],
		ChangelogEntries:    changelogEntries,
		ChangelogPagination: pagination,
		PageTitle:           p.Name,
	}
	renderTemplate(w, r, "character_detail.html", data)
}

func characterChangelogHandler(w http.ResponseWriter, r *http.Request) {
	const entriesPerPage = 100
	var totalEntries int
	if err := db.QueryRow("SELECT COUNT(*) FROM character_changelog").Scan(&totalEntries); err != nil {
		http.Error(w, "Could not count changelog entries", http.StatusInternalServerError)
		return
	}

	pagination := newPaginationData(r, totalEntries, entriesPerPage)
	query := `SELECT character_name, change_time, activity_description FROM character_changelog ORDER BY change_time DESC LIMIT ? OFFSET ?`
	rows, err := db.Query(query, entriesPerPage, pagination.Offset)
	if err != nil {
		http.Error(w, "Could not query for character changelog", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var changelogEntries []CharacterChangelog
	for rows.Next() {
		var entry CharacterChangelog
		var timestampStr string
		if err := rows.Scan(&entry.CharacterName, &timestampStr, &entry.ActivityDescription); err != nil {
			log.Printf("[W] [HTTP/Changelog] Failed to scan character changelog row: %v", err)
			continue
		}
		if parsedTime, err := time.Parse(time.RFC3339, timestampStr); err == nil {
			entry.ChangeTime = parsedTime.Format("2006-01-02 15:04:05")
		} else {
			entry.ChangeTime = timestampStr
		}
		changelogEntries = append(changelogEntries, entry)
	}

	data := CharacterChangelogPageData{
		ChangelogEntries: changelogEntries,
		LastScrapeTime:   GetLastScrapeTime(),
		Pagination:       pagination,
		PageTitle:        "Character Changelog",
	}
	renderTemplate(w, r, "character_changelog.html", data)
}

func guildDetailHandler(w http.ResponseWriter, r *http.Request) {
	guildName := r.URL.Query().Get("name")
	if guildName == "" {
		http.Error(w, "Guild name is required", http.StatusBadRequest)
		return
	}
	const entriesPerPage = 25

	var g Guild
	guildQuery := `
        SELECT name, level, experience, master, emblem_url,
            (SELECT COUNT(*) FROM characters WHERE guild_name = guilds.name),
            COALESCE((SELECT SUM(zeny) FROM characters WHERE guild_name = guilds.name), 0),
            COALESCE((SELECT AVG(base_level) FROM characters WHERE guild_name = guilds.name), 0)
        FROM guilds WHERE name = ?`

	err := db.QueryRow(guildQuery, guildName).Scan(&g.Name, &g.Level, &g.Experience, &g.Master, &g.EmblemURL, &g.MemberCount, &g.TotalZeny, &g.AvgBaseLevel)
	if err != nil {
		if err == sql.ErrNoRows {
			http.Error(w, "Guild not found", http.StatusNotFound)
		} else {
			http.Error(w, "Could not query for guild details", http.StatusInternalServerError)
		}
		return
	}

	allowedSorts := map[string]string{
		"rank": "rank", "name": "name", "base_level": "base_level", "job_level": "job_level",
		"experience": "experience", "zeny": "zeny", "class": "class", "last_active": "last_active",
	}
	orderByClause, sortBy, order := getSortClause(r, allowedSorts, "base_level", "DESC")

	membersQuery := fmt.Sprintf(`SELECT rank, name, base_level, job_level, experience, class, zeny, last_active FROM characters
		WHERE guild_name = ? %s`, orderByClause)
	rows, err := db.Query(membersQuery, guildName)
	if err != nil {
		http.Error(w, "Could not query for guild members", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var members []PlayerCharacter
	classDistribution := make(map[string]int)
	for rows.Next() {
		var p PlayerCharacter
		var lastActiveStr string
		if err := rows.Scan(&p.Rank, &p.Name, &p.BaseLevel, &p.JobLevel, &p.Experience, &p.Class, &p.Zeny, &lastActiveStr); err != nil {
			log.Printf("[W] [HTTP/Guild] Failed to scan guild member row: %v", err)
			continue
		}
		classDistribution[p.Class]++
		if t, err := time.Parse(time.RFC3339, lastActiveStr); err == nil {
			p.LastActive = t.Format("2006-01-02 15:04")
		}
		p.IsGuildLeader = (p.Name == g.Master)
		members = append(members, p)
	}
	classDistJSON, _ := json.Marshal(classDistribution)

	likePattern := "%" + guildName + "%"
	var totalChangelogEntries int
	db.QueryRow("SELECT COUNT(*) FROM character_changelog WHERE activity_description LIKE ?", likePattern).Scan(&totalChangelogEntries)

	pagination := newPaginationData(r, totalChangelogEntries, entriesPerPage)
	changelogQuery := `SELECT change_time, character_name, activity_description FROM character_changelog
        WHERE activity_description LIKE ? ORDER BY change_time DESC LIMIT ? OFFSET ?`
	changelogRows, err := db.Query(changelogQuery, likePattern, entriesPerPage, pagination.Offset)
	if err != nil {
		http.Error(w, "Could not query for guild changelog", http.StatusInternalServerError)
		return
	}
	defer changelogRows.Close()

	var changelogEntries []CharacterChangelog
	for changelogRows.Next() {
		var entry CharacterChangelog
		var timestampStr string
		if err := changelogRows.Scan(&timestampStr, &entry.CharacterName, &entry.ActivityDescription); err == nil {
			if t, err := time.Parse(time.RFC3339, timestampStr); err == nil {
				entry.ChangeTime = t.Format("2006-01-02 15:04:05")
			}
			changelogEntries = append(changelogEntries, entry)
		}
	}

	data := GuildDetailPageData{
		Guild:                 g,
		Members:               members,
		LastScrapeTime:        GetLastScrapeTime(),
		SortBy:                sortBy,
		Order:                 order,
		ClassDistributionJSON: template.JS(classDistJSON),
		HasChartData:          len(classDistribution) > 1,
		ChangelogEntries:      changelogEntries,
		ChangelogPagination:   pagination,
		PageTitle:             g.Name,
	}
	renderTemplate(w, r, "guild_detail.html", data)
}

func storeDetailHandler(w http.ResponseWriter, r *http.Request) {
	storeName := r.URL.Query().Get("name")
	sellerNameQuery := r.URL.Query().Get("seller")
	if storeName == "" {
		http.Error(w, "Store name is required", http.StatusBadRequest)
		return
	}

	allowedSorts := map[string]string{
		"name": "name_of_the_item", "item_id": "item_id", "quantity": "quantity",
		"price": "CAST(REPLACE(price, ',', '') AS INTEGER)",
	}
	orderByClause, sortBy, order := getSortClause(r, allowedSorts, "price", "DESC")

	var sellerName, mapName, mapCoords, mostRecentTimestampStr string
	var signatureQueryArgs []interface{}
	signatureQuery := `SELECT seller_name, map_name, map_coordinates, date_and_time_retrieved FROM items WHERE store_name = ?`
	signatureQueryArgs = append(signatureQueryArgs, storeName)
	if sellerNameQuery != "" {
		signatureQuery += " AND seller_name = ?"
		signatureQueryArgs = append(signatureQueryArgs, sellerNameQuery)
	}
	signatureQuery += ` ORDER BY date_and_time_retrieved DESC, id DESC LIMIT 1`
	err := db.QueryRow(signatureQuery, signatureQueryArgs...).Scan(&sellerName, &mapName, &mapCoords, &mostRecentTimestampStr)

	var items []Item
	if err == nil {
		// --- MODIFICATION: Join internal_item_db ---
		query := fmt.Sprintf(`
			WITH RankedItems AS (
				SELECT i.*, local_db.name_pt, ROW_NUMBER() OVER(PARTITION BY i.name_of_the_item ORDER BY i.id DESC) as rn
				FROM items i
				LEFT JOIN internal_item_db local_db ON i.item_id = local_db.item_id
				WHERE i.store_name = ? AND i.seller_name = ? AND i.map_name = ? AND i.map_coordinates = ?
			)
			SELECT id, name_of_the_item, name_pt, item_id, quantity, price, store_name, seller_name, date_and_time_retrieved, map_name, map_coordinates, is_available
			FROM RankedItems WHERE rn = 1 %s`, orderByClause)
		// --- END MODIFICATION ---

		rows, queryErr := db.Query(query, storeName, sellerName, mapName, mapCoords)
		if queryErr != nil {
			http.Error(w, "Could not query for store items", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var item Item
			var retrievedTime string
			if err := rows.Scan(&item.ID, &item.Name, &item.NamePT, &item.ItemID, &item.Quantity, &item.Price, &item.StoreName, &item.SellerName, &retrievedTime, &item.MapName, &item.MapCoordinates, &item.IsAvailable); err == nil {
				if t, err := time.Parse(time.RFC3339, retrievedTime); err == nil {
					item.Timestamp = t.Format("2006-01-02 15:04")
				}
				items = append(items, item)
			}
		}
	} else if err != sql.ErrNoRows {
		http.Error(w, "Database error finding store", http.StatusInternalServerError)
		return
	}

	data := StoreDetailPageData{
		StoreName:      storeName,
		SellerName:     sellerName,
		MapName:        strings.ToLower(mapName),
		MapCoordinates: mapCoords,
		Items:          items,
		LastScrapeTime: GetLastScrapeTime(),
		SortBy:         sortBy,
		Order:          order,
		PageTitle:      storeName,
	}
	renderTemplate(w, r, "store_detail.html", data)
}

func generateSecretToken(length int) (string, error) {
	bytes := make([]byte, length)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func tradingPostListHandler(w http.ResponseWriter, r *http.Request) {
	searchQuery := r.URL.Query().Get("query")
	filterType := r.URL.Query().Get("filter_type")
	if filterType == "" {
		filterType = "all"
	}
	filterCurrency := r.URL.Query().Get("filter_currency")
	if filterCurrency == "" {
		filterCurrency = "all"
	}

	var queryParams []interface{}
	// --- MODIFICATION: Join internal_item_db ---
	baseQuery := `
		SELECT
			p.id, p.title, p.post_type, p.character_name, p.contact_info, p.created_at, p.notes,
			i.item_name, local_db.name_pt, i.item_id, i.quantity, i.price_zeny, i.price_rmt, 
			i.payment_methods, i.refinement, i.card1, i.card2, i.card3, i.card4
		FROM trading_post_items i
		JOIN trading_posts p ON i.post_id = p.id
		LEFT JOIN internal_item_db local_db ON i.item_id = local_db.item_id
	`
	// --- END MODIFICATION ---
	var whereConditions []string

	if searchQuery != "" {

		if _, err := strconv.Atoi(searchQuery); err == nil {
			whereConditions = append(whereConditions, "i.item_id = ?")
			queryParams = append(queryParams, searchQuery)
		} else {

			idList, _ := getCombinedItemIDs(searchQuery)

			var nameClauses []string
			nameClauses = append(nameClauses, "i.item_name LIKE ?")
			queryParams = append(queryParams, "%"+searchQuery+"%")

			if len(idList) > 0 {
				placeholders := strings.Repeat("?,", len(idList)-1) + "?"
				nameClauses = append(nameClauses, fmt.Sprintf("i.item_id IN (%s)", placeholders))
				for _, id := range idList {
					queryParams = append(queryParams, id)
				}
			}
			whereConditions = append(whereConditions, "("+strings.Join(nameClauses, " OR ")+")")
		}
	}

	if filterType == "selling" {
		whereConditions = append(whereConditions, "p.post_type = ?")
		queryParams = append(queryParams, "selling")
	} else if filterType == "buying" {
		whereConditions = append(whereConditions, "p.post_type = ?")
		queryParams = append(queryParams, "buying")
	}

	if filterCurrency == "zeny" {

		whereConditions = append(whereConditions, "(i.price_zeny > 0 OR i.payment_methods IN ('zeny', 'both'))")
	} else if filterCurrency == "rmt" {

		whereConditions = append(whereConditions, "(i.price_rmt > 0 OR i.payment_methods IN ('rmt', 'both'))")
	}

	whereClause := ""
	if len(whereConditions) > 0 {
		whereClause = "WHERE " + strings.Join(whereConditions, " AND ")
	}

	allowedSorts := map[string]string{
		"item_name": "i.item_name",
		"quantity":  "i.quantity",
		"seller":    "p.character_name",
		"posted":    "p.created_at",
	}

	if filterCurrency == "rmt" {
		allowedSorts["price"] = "CASE WHEN i.price_rmt = 0 THEN 9223372036854775807 ELSE i.price_rmt END"
	} else {
		allowedSorts["price"] = "CASE WHEN i.price_zeny = 0 THEN 9223372036854775807 ELSE i.price_zeny END"
	}

	orderByClause, sortBy, order := getSortClause(r, allowedSorts, "posted", "DESC")

	finalQuery := baseQuery + whereClause + " " + orderByClause

	rows, err := db.Query(finalQuery, queryParams...)
	if err != nil {
		log.Printf("[E] [HTTP/Trade] Trading Post flat list query error: %v", err)
		http.Error(w, "Database query failed", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var items []FlatTradingPostItem
	for rows.Next() {
		var item FlatTradingPostItem
		err := rows.Scan(
			&item.PostID, &item.Title, &item.PostType, &item.CharacterName, &item.ContactInfo, &item.CreatedAt, &item.Notes,
			&item.ItemName, &item.NamePT, &item.ItemID, &item.Quantity, &item.PriceZeny, &item.PriceRMT,
			&item.PaymentMethods,
			&item.Refinement, &item.Card1, &item.Card2, &item.Card3, &item.Card4,
		)
		if err != nil {
			log.Printf("[W] [HTTP/Trade] Failed to scan flat trading post item: %v", err)
			continue
		}
		items = append(items, item)
	}

	data := TradingPostPageData{
		Items:          items,
		LastScrapeTime: GetLastScrapeTime(),
		SearchQuery:    searchQuery,
		FilterType:     filterType,
		FilterCurrency: filterCurrency,
		SortBy:         sortBy,
		Order:          order,
		PageTitle:      "Discord",
	}
	renderTemplate(w, r, "trading_post.html", data)
}

// findItemIDInCache attempts to find an item ID using the local item DB.
// This version uses LIKE and Levenshtein distance for proximity matching.
// --- MODIFICATION: Added 'slots' parameter ---
func findItemIDInCache(cleanItemName string, slots int) (sql.NullInt64, bool) {

	// --- MODIFICATION: Build case-insensitive query ---
	// Lowercase the search term *once*
	lowerCleanItemName := strings.ToLower(cleanItemName)
	likeQuery := "%" + strings.ReplaceAll(lowerCleanItemName, " ", "%") + "%"

	var queryParams []interface{}
	queryParams = append(queryParams, likeQuery, likeQuery)

	var slotClause string
	if slots == 0 {
		// If slots are 0, match 0 or NULL (since NULL slots also mean 0)
		slotClause = "AND (slots = 0 OR slots IS NULL)"
	} else {
		// If slots > 0, match that exact number
		slotClause = "AND slots = ?"
		queryParams = append(queryParams, slots)
	}

	// Use LOWER() on columns for case-insensitive matching
	query := fmt.Sprintf(`
		SELECT item_id, name, name_pt 
		FROM internal_item_db 
		WHERE (LOWER(name) LIKE ? OR LOWER(name_pt) LIKE ?)
		%s
		LIMIT 10`, slotClause) // Limit to 10 potential matches

	rows, err := db.Query(query, queryParams...)
	// --- END MODIFICATION ---
	if err != nil {
		log.Printf("[W] [ItemID] Error during LIKE query for '%s' (slots: %d): %v", likeQuery, slots, err)
		return sql.NullInt64{Valid: false}, false
	}
	defer rows.Close()

	type potentialMatch struct {
		id     int64
		name   string // This is the original name from the DB (e.g., "Jur")
		namePT string // This is the original PT name (e.g., "Jur")
	}
	var potentialMatches []potentialMatch

	for rows.Next() {
		var match potentialMatch
		var namePT sql.NullString
		if err := rows.Scan(&match.id, &match.name, &namePT); err == nil {
			if namePT.Valid {
				match.namePT = namePT.String
			}
			potentialMatches = append(potentialMatches, match)
		}
	}

	if len(potentialMatches) == 1 {
		itemID := potentialMatches[0].id
		log.Printf("[D] [ItemID] Found unique local LIKE match for '%s': ID %d", cleanItemName, itemID)
		return sql.NullInt64{Int64: itemID, Valid: true}, true
	}

	// Disambiguation logic (Levenshtein) remains the same as before
	// We already have lowerCleanItemName from above.

	// 1. Check for a perfect match first
	for _, match := range potentialMatches {
		if strings.ToLower(match.name) == lowerCleanItemName || strings.ToLower(match.namePT) == lowerCleanItemName {
			log.Printf("[D] [ItemID] Found perfect match '%s' (ID %d) within %d LIKE results.", cleanItemName, match.id, len(potentialMatches))
			return sql.NullInt64{Int64: match.id, Valid: true}, true
		}
	}

	// 2. No perfect match, find the closest Levenshtein distance.
	const maxLevenshteinDistance = 2

	bestMatchID := int64(-1)
	bestMatchName := ""
	minDistance := 100

	log.Printf("[D] [ItemID/Levenshtein] No perfect match for '%s'. Calculating proximity for %d candidates.", cleanItemName, len(potentialMatches))

	for _, match := range potentialMatches {
		lowerNameEN := strings.ToLower(match.name)
		distEN := levenshtein.ComputeDistance(lowerCleanItemName, lowerNameEN)

		currentBestName := match.name
		currentMinDist := distEN

		logMsg := fmt.Sprintf("[D] [ItemID/Levenshtein] ...vs EN '%s' (ID %d): dist %d.", lowerNameEN, match.id, distEN)

		if match.namePT != "" {
			lowerNamePT := strings.ToLower(match.namePT)
			distPT := levenshtein.ComputeDistance(lowerCleanItemName, lowerNamePT)
			logMsg += fmt.Sprintf(" vs PT '%s': dist %d.", lowerNamePT, distPT)

			if distPT < currentMinDist {
				currentMinDist = distPT
				currentBestName = match.namePT
			}
		}

		log.Print(logMsg)

		if currentMinDist < minDistance {
			minDistance = currentMinDist
			bestMatchID = match.id
			bestMatchName = currentBestName
		}
	}

	log.Printf("[DF] [ItemID/Levenshtein] Best proximity match for '%s' is '%s' (ID %d) with distance %d.", cleanItemName, bestMatchName, bestMatchID, minDistance)

	// 3. Check if the best match is within our acceptable threshold
	if bestMatchID != -1 && minDistance <= maxLevenshteinDistance {
		log.Printf("[D] [ItemID] Accepting proximity match for '%s' (ID %d). Distance %d is <= %d.", cleanItemName, bestMatchID, minDistance, maxLevenshteinDistance)
		return sql.NullInt64{Int64: bestMatchID, Valid: true}, true
	}

	// 4. Fallback for cards (unchanged)
	if strings.Contains(lowerCleanItemName, "card") || strings.Contains(lowerCleanItemName, "carta") {
		if len(potentialMatches) > 0 {
			itemID := potentialMatches[0].id // Trust the first result from LIKE
			log.Printf("[D] [ItemID] Found %d LIKE matches for '%s'. Proximity match rejected (dist %d > %d). Using first result (ID %d) due to 'card'/'carta' keyword.", len(potentialMatches), cleanItemName, minDistance, maxLevenshteinDistance, itemID)
			return sql.NullInt64{Int64: itemID, Valid: true}, true
		}
	}

	// 5. All ambiguity checks failed.
	log.Printf("[D] [ItemID] Found %d ambiguous LIKE matches for '%s'. Proximity match rejected (dist %d > %d). Proceeding to online search.", len(potentialMatches), cleanItemName, minDistance, maxLevenshteinDistance)
	return sql.NullInt64{Valid: false}, false
}

// findItemIDOnline performs concurrent web scrapes to find an item ID.
func findItemIDOnline(cleanItemName string, slots int) (sql.NullInt64, bool) {
	log.Printf("[D] [ItemID] No local FTS match for '%s'. Initiating online search...", cleanItemName)

	var wg sync.WaitGroup
	// --- MODIFICATION: Removed rmsResults and rmsErr ---
	var rdbResults []ItemSearchResult
	var rodbErr error

	// --- MODIFICATION: Removed one item from wg.Add() ---
	wg.Add(1)
	// --- MODIFICATION: Removed goroutine for scrapeRMSItemSearch ---
	// go func() { ... }()
	// --- END MODIFICATION ---
	go func() {
		defer wg.Done()
		rdbResults, rodbErr = scrapeRODatabaseSearch(cleanItemName, slots)
		if rodbErr != nil {
			log.Printf("[W] [ItemID] RDB Search failed for '%s': %v", cleanItemName, rodbErr)
		}
	}()
	wg.Wait()

	combinedIDs := make(map[int]string)
	// --- MODIFICATION: Removed loop for rmsResults ---
	if rdbResults != nil {
		for _, res := range rdbResults {
			if _, ok := combinedIDs[res.ID]; !ok {
				combinedIDs[res.ID] = res.Name
			}
		}
	}
	// --- END MODIFICATION ---

	if len(combinedIDs) == 1 {
		var foundID int
		// var foundName string // No longer needed
		for id, name := range combinedIDs {
			foundID = id
			_ = name // foundName = name
		}
		log.Printf("[D] [ItemID] Found unique ONLINE match for '%s': ID %d", cleanItemName, foundID)
		// --- MODIFICATION: Removed background caching ---
		// go scrapeAndCacheItemIfNotExists(foundID, foundName) // This function no longer exists
		// --- END MODIFICATION ---
		return sql.NullInt64{Int64: int64(foundID), Valid: true}, true
	}

	if len(combinedIDs) > 1 {
		// Check for a perfect match among online results
		for id, name := range combinedIDs {
			nameWithoutSlots := reSlotRemover.ReplaceAllString(name, " ")
			nameWithoutSlots = strings.TrimSpace(nameWithoutSlots)

			if name == cleanItemName || (nameWithoutSlots != "" && nameWithoutSlots == cleanItemName) {
				log.Printf("[D] [ItemID] Found perfect match (exact or slot-stripped) '%s' (ID %d) within ONLINE results.", cleanItemName, id)
				// --- MODIFICATION: Removed background caching ---
				// go scrapeAndCacheItemIfNotExists(id, name) // This function no longer exists
				// --- END MODIFICATION ---
				return sql.NullInt64{Int64: int64(id), Valid: true}, true
			}
		}

		// Fallback for cards: trust the first result
		lowerCleanItemName := strings.ToLower(cleanItemName)
		if strings.Contains(lowerCleanItemName, "card") || strings.Contains(lowerCleanItemName, "carta") {
			var foundID int
			// var foundName string // No longer needed
			for id, name := range combinedIDs {
				foundID = id
				_ = name // foundName = name
				break    // Get the first one
			}
			log.Printf("[D] [ItemID] Found %d ONLINE matches for '%s'. Using first result (ID %d) due to 'card'/'carta' keyword.", len(combinedIDs), cleanItemName, foundID)
			// --- MODIFICATION: Removed background caching ---
			// go scrapeAndCacheItemIfNotExists(foundID, foundName) // This function no longer exists
			// --- END MODIFICATION ---
			return sql.NullInt64{Int64: int64(foundID), Valid: true}, true
		}

		log.Printf("[D] [ItemID] Found %d ambiguous ONLINE matches for '%s'. Not selecting.", len(combinedIDs), cleanItemName)
	}

	// No online matches or ambiguous matches
	return sql.NullInt64{Valid: false}, false
}

// woeRankingsHandler fetches and displays WoE rankings for characters or guilds.
func woeRankingsHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)
		return
	}
	searchQuery := r.FormValue("query")
	activeTab := r.FormValue("tab")
	if activeTab == "" {
		activeTab = "characters" // Default to character view
	}

	var characters []WoeCharacterRank
	var guilds []WoeGuildRank
	var allowedSorts map[string]string
	var orderByClause, sortBy, order string
	var whereConditions []string
	var queryParams []interface{}
	var whereClause string

	if activeTab == "guilds" {
		// --- GUILD RANKING LOGIC ---
		allowedSorts = map[string]string{
			"guild":    "guild_name",
			"members":  "member_count",
			"kills":    "total_kills",
			"deaths":   "total_deaths",
			"kd":       "kd_ratio",
			"damage":   "total_damage",
			"healing":  "total_healing",
			"emperium": "total_emp_kills",
			"points":   "total_points",
		}
		// Default sort by total kills DESC
		orderByClause, sortBy, order = getSortClause(r, allowedSorts, "kills", "DESC")

		if searchQuery != "" {
			whereConditions = append(whereConditions, "guild_name LIKE ?")
			queryParams = append(queryParams, "%"+searchQuery+"%")
		}
		if len(whereConditions) > 0 {
			whereClause = "WHERE " + strings.Join(whereConditions, " AND ")
		}

		// Query to aggregate stats by guild
		query := fmt.Sprintf(`
			SELECT
				guild_name,
				guild_id,
				COUNT(*) AS member_count,
				SUM(kill_count) AS total_kills,
				SUM(death_count) AS total_deaths,
				SUM(damage_done) AS total_damage,
				SUM(healing_done) AS total_healing,
				SUM(emperium_kill) AS total_emp_kills,
				SUM(points) AS total_points,
				CASE
					WHEN SUM(death_count) = 0 THEN SUM(kill_count) -- Avoid division by zero
					ELSE CAST(SUM(kill_count) AS REAL) / SUM(death_count)
				END AS kd_ratio
			FROM woe_character_rankings
			%s -- whereClause
			GROUP BY guild_name, guild_id
			%s -- orderByClause
		`, whereClause, orderByClause)

		rows, err := db.Query(query, queryParams...)
		if err != nil {
			log.Printf("[E] [HTTP/WoE] Could not query for WoE guild rankings: %v | Query: %s | Params: %v", err, query, queryParams)
			http.Error(w, "Could not query WoE guild rankings", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var g WoeGuildRank
			err := rows.Scan(
				&g.GuildName, &g.GuildID, &g.MemberCount,
				&g.TotalKills, &g.TotalDeaths, &g.TotalDamage,
				&g.TotalHealing, &g.TotalEmpKills, &g.TotalPoints,
				&g.KillDeathRatio,
			)
			if err != nil {
				log.Printf("[W] [HTTP/WoE] Failed to scan WoE guild row: %v", err)
				continue
			}
			guilds = append(guilds, g)
		}

	} else {
		// --- CHARACTER RANKING LOGIC (Original logic) ---
		allowedSorts = map[string]string{
			"name":     "name",
			"class":    "class",
			"guild":    "guild_name",
			"kills":    "kill_count",
			"deaths":   "death_count",
			"damage":   "damage_done",
			"emperium": "emperium_kill",
			"healing":  "healing_done",
			"score":    "score",
			"points":   "points",
		}
		// Default sort by kills DESC
		orderByClause, sortBy, order = getSortClause(r, allowedSorts, "kills", "DESC")

		if searchQuery != "" {
			// Use LIKE for partial matching
			whereConditions = append(whereConditions, "name LIKE ?")
			queryParams = append(queryParams, "%"+searchQuery+"%")
		}
		if len(whereConditions) > 0 {
			whereClause = "WHERE " + strings.Join(whereConditions, " AND ")
		}

		query := fmt.Sprintf(`
			SELECT name, class, guild_id, guild_name,
				   kill_count, death_count, damage_done, emperium_kill,
				   healing_done, score, points
			FROM woe_character_rankings
			%s -- whereClause
			%s -- orderByClause
		`, whereClause, orderByClause)

		rows, err := db.Query(query, queryParams...)
		if err != nil {
			log.Printf("[E] [HTTP/WoE] Could not query for WoE character rankings: %v | Query: %s | Params: %v", err, query, queryParams)
			http.Error(w, "Could not query WoE rankings", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var c WoeCharacterRank
			err := rows.Scan(
				&c.Name, &c.Class, &c.GuildID, &c.GuildName,
				&c.KillCount, &c.DeathCount, &c.DamageDone, &c.EmperiumKill,
				&c.HealingDone, &c.Score, &c.Points,
			)
			if err != nil {
				log.Printf("[W] [HTTP/WoE] Failed to scan WoE character row: %v", err)
				continue
			}
			characters = append(characters, c)
		}
	}

	data := WoePageData{
		Characters:     characters,
		Guilds:         guilds,
		ActiveTab:      activeTab,
		LastScrapeTime: GetLastUpdateTime("last_updated", "woe_character_rankings"),
		SortBy:         sortBy,
		Order:          order,
		SearchQuery:    searchQuery,
		PageTitle:      "WoE Rankings",
	}
	renderTemplate(w, r, "woe_rankings.html", data)
}

type ChatActivityPoint struct {
	Timestamp string `json:"t"`
	Value     int    `json:"v"`
}

// getChatActivityGraphData queries the DB for activity heartbeats in the
// last 24 hours and formats them for a Chart.js graph.
func getChatActivityGraphData() template.JS {
	now := time.Now()
	// Start from exactly 24 hours ago, truncated to the minute
	viewStart := now.Add(-24 * time.Hour).Truncate(time.Minute)

	// 1. Get all heartbeats from the DB in the time range
	rows, err := db.Query("SELECT timestamp FROM chat_activity_log WHERE timestamp >= ?", viewStart.Format(time.RFC3339))
	if err != nil {
		log.Printf("[E] [HTTP/Chat] Could not query chat activity log: %v", err)
		return template.JS("[]")
	}
	defer rows.Close()

	// Store found heartbeats in a map for fast lookup
	activeMinutes := make(map[string]bool)
	for rows.Next() {
		var ts string
		if err := rows.Scan(&ts); err == nil {
			activeMinutes[ts] = true
		}
	}

	// 2. Build the full 1440-point (24 * 60) dataset
	var graphData []ChatActivityPoint
	currentMinute := viewStart

	// Loop from 24 hours ago up to the current minute, in 3-minute steps
	for currentMinute.Before(now) {
		timestampStr := currentMinute.Format(time.RFC3339)

		// Check the next two minutes as well to cover the 3-minute window
		minute2 := currentMinute.Add(1 * time.Minute)
		minute2Str := minute2.Format(time.RFC3339)
		minute3 := currentMinute.Add(2 * time.Minute)
		minute3Str := minute3.Format(time.RFC3339)

		value := 0
		// Check if *any* minute in the 3-minute window has activity
		if activeMinutes[timestampStr] || activeMinutes[minute2Str] || activeMinutes[minute3Str] {
			value = 1 // Active
		}

		graphData = append(graphData, ChatActivityPoint{
			Timestamp: timestampStr, // The timestamp for the *start* of the 3-minute bucket
			Value:     value,
		})

		// Increment by 3 minutes for the next bucket
		currentMinute = currentMinute.Add(3 * time.Minute)
	}

	// 3. Marshal to JSON
	jsonData, err := json.Marshal(graphData)
	if err != nil {
		log.Printf("[E] [HTTP/Chat] Could not marshal activity graph data: %v", err)
		return template.JS("[]")
	}

	return template.JS(jsonData)
}

// chatHandler displays the paginated public chat log.
func chatHandler(w http.ResponseWriter, r *http.Request) {
	const messagesPerPage = 100 // 100 messages per page

	// ... (channel and search query logic is unchanged) ...
	activeChannel := r.URL.Query().Get("channel")
	searchQuery := r.URL.Query().Get("query")
	if activeChannel == "" || activeChannel == "Local" {
		activeChannel = "all"
	}

	// ... (all channel, WHERE clause, and pagination logic is unchanged) ...
	var allChannels []string
	channelRows, err := db.Query("SELECT DISTINCT channel FROM chat WHERE channel != 'Local' ORDER BY channel ASC")
	if err != nil {
		log.Printf("[W] [HTTP/Chat] Could not query for distinct channels: %v", err)
	} else {
		for channelRows.Next() {
			var channel string
			if err := channelRows.Scan(&channel); err == nil {
				allChannels = append(allChannels, channel)
			}
		}
		channelRows.Close()
	}

	var whereConditions []string
	var params []interface{}
	if activeChannel != "all" {
		whereConditions = append(whereConditions, "channel = ?")
		params = append(params, activeChannel)
	} else {
		whereConditions = append(whereConditions, "channel != ?")
		params = append(params, "Local")
	}

	if searchQuery != "" {
		whereConditions = append(whereConditions, "(message LIKE ? OR character_name LIKE ?)")
		likeQuery := "%" + searchQuery + "%"
		params = append(params, likeQuery, likeQuery)
	}

	whereConditions = append(whereConditions, "NOT (channel = 'Drop' AND character_name = 'System' AND (message LIKE '%Os Campos de Batalha%' OR message LIKE '%Utilizem os efeitos%'))")

	whereClause := ""
	if len(whereConditions) > 0 {
		whereClause = "WHERE " + strings.Join(whereConditions, " AND ")
	}

	var totalMessages int
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM chat %s", whereClause)
	if err := db.QueryRow(countQuery, params...).Scan(&totalMessages); err != nil {
		log.Printf("[E] [HTTP/Chat] Could not count chat messages: %v", err)
		http.Error(w, "Could not count chat messages", http.StatusInternalServerError)
		return
	}

	pagination := newPaginationData(r, totalMessages, messagesPerPage)

	query := fmt.Sprintf(`
		SELECT timestamp, channel, character_name, message 
		FROM chat 
		%s
		ORDER BY timestamp DESC 
		LIMIT ? OFFSET ?`, whereClause)

	queryArgs := params
	queryArgs = append(queryArgs, messagesPerPage, pagination.Offset)
	// --- END FIX ---
	rows, err := db.Query(query, queryArgs...)

	if err != nil {
		log.Printf("[E] [HTTP/Chat] Could not query for chat messages: %v", err)
		http.Error(w, "Could not query for chat messages", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var messages []ChatMessage
	for rows.Next() {
		var msg ChatMessage
		var timestampStr string
		if err := rows.Scan(&timestampStr, &msg.Channel, &msg.CharacterName, &msg.Message); err != nil {
			log.Printf("[W] [HTTP/Chat] Failed to scan chat message row: %v", err)
			continue
		}
		if parsedTime, err := time.Parse(time.RFC3339, timestampStr); err == nil {
			msg.Timestamp = parsedTime.Format("2006-01-02 15:04:05")
		} else {
			msg.Timestamp = timestampStr
		}
		messages = append(messages, msg)
	}

	queryFilter := url.Values{}
	// --- FIX: Always add the channel filter if it's not the default "all" ---
	if activeChannel != "all" {
		queryFilter.Set("channel", activeChannel)
	}
	// --- END FIX ---
	if searchQuery != "" {
		queryFilter.Set("query", searchQuery)
	}

	// --- END UNCHANGED SECTION ---

	var filterString string
	if encodedFilter := queryFilter.Encode(); encodedFilter != "" {
		filterString = "&" + encodedFilter
	}

	// --- MODIFICATION: Get graph data ---
	activityGraphJSON := getChatActivityGraphData()
	// --- END MODIFICATION ---

	data := ChatPageData{
		Messages:       messages,
		LastScrapeTime: GetLastChatPacketTime(),
		Pagination:     pagination,
		PageTitle:      "Chat",
		AllChannels:    allChannels,
		ActiveChannel:  activeChannel,
		QueryFilter:    template.URL(filterString), // <-- Use the corrected string
		SearchQuery:    searchQuery,
		// --- MODIFICATION: Pass data to template ---
		ActivityGraphJSON: activityGraphJSON,
	}
	renderTemplate(w, r, "chat.html", data)
}

// aboutHandler displays the static "About" page.
func aboutHandler(w http.ResponseWriter, r *http.Request) {
	// We pass the PageTitle so the navbar can highlight the correct link.
	data := map[string]interface{}{
		"PageTitle": "About",
	}
	renderTemplate(w, r, "about.html", data)
}

// findItemIDByName orchestrates searching the cache and online for an item ID.
// This function remains unchanged but is shown for context.
func findItemIDByName(itemName string, allowRetry bool, slots int) (sql.NullInt64, error) {
	// 1. Clean the name
	reRefine := regexp.MustCompile(`\s*\+\d+\s*`)
	cleanItemName := reSlotRemover.ReplaceAllString(itemName, " ")
	cleanItemName = reRefine.ReplaceAllString(cleanItemName, "")
	cleanItemName = strings.TrimSpace(cleanItemName)
	cleanItemName = sanitizeString(cleanItemName, itemSanitizer)

	if strings.TrimSpace(cleanItemName) == "" {
		return sql.NullInt64{Valid: false}, nil
	}

	// 2. Handle special case: "Zeny"
	if strings.ToLower(cleanItemName) == "zeny" {
		log.Printf("[D] [ItemID] Detected special item 'Zeny'. Skipping ID search.")
		return sql.NullInt64{Valid: false}, nil
	}

	// 3. Try local FTS cache first (This is the modified function)
	// --- MODIFICATION: Pass 'slots' parameter ---
	if itemID, found := findItemIDInCache(cleanItemName, slots); found {
		// --- END MODIFICATION ---
		return itemID, nil
	}

	// 4. If not found, try online search
	if itemID, found := findItemIDOnline(cleanItemName, slots); found {
		return itemID, nil
	}

	// 5. If still not found, handle retry logic for "card" or "carta"
	lowerCleanItemName := strings.ToLower(cleanItemName)
	isCard := strings.Contains(lowerCleanItemName, "card") || strings.Contains(lowerCleanItemName, "carta")

	if allowRetry && isCard {
		newName := reCardRemover.ReplaceAllString(itemName, " ")
		newName = strings.TrimSpace(newName)

		if newName != "" && newName != cleanItemName {
			log.Printf("[D] [ItemID] No results for '%s'. Retrying search without 'card'/'carta' as: '%s'", cleanItemName, newName)
			// Call recursively, but with allowRetry=false to prevent infinite loops
			return findItemIDByName(newName, false, slots)
		}
	}

	// 6. All attempts failed
	log.Printf("[D] [ItemID] All searches for '%s' returned no results or were ambiguous. Storing name only.", cleanItemName)
	return sql.NullInt64{Valid: false}, nil
}

// findAndDeleteOldPosts performs a pre-transaction to clean up duplicate posts.
func findAndDeleteOldPosts(tx *sql.Tx, characterName, discordContact, postType string, itemNames []string, itemParams []interface{}) {
	if len(itemNames) == 0 {
		return
	}

	placeholders := strings.Repeat("?,", len(itemNames)-1) + "?"
	findQuery := fmt.Sprintf(`
		SELECT DISTINCT p.id
		FROM trading_posts p
		JOIN trading_post_items i ON p.id = i.post_id
		WHERE p.character_name = ?
		  AND p.contact_info = ?
		  AND p.post_type = ?
		  AND i.item_name IN (%s)
	`, placeholders)

	findParams := []interface{}{characterName, discordContact, postType}
	findParams = append(findParams, itemParams...)

	rows, err := tx.Query(findQuery, findParams...)
	if err != nil {
		log.Printf("[W] [Discord] Failed to query for old posts to delete for '%s': %v", characterName, err)
		return // Don't fail the transaction, just log the error
	}
	defer rows.Close()

	var postIDsToDelete []interface{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err == nil {
			postIDsToDelete = append(postIDsToDelete, id)
		}
	}

	if len(postIDsToDelete) > 0 {
		delPlaceholders := strings.Repeat("?,", len(postIDsToDelete)-1) + "?"
		delQuery := fmt.Sprintf("DELETE FROM trading_posts WHERE id IN (%s)", delPlaceholders)

		delRes, err := tx.Exec(delQuery, postIDsToDelete...)
		if err != nil {
			log.Printf("[W] [Discord] Failed to delete old post(s) for '%s': %v", characterName, err)
		} else if deletedCount, _ := delRes.RowsAffected(); deletedCount > 0 {
			log.Printf("[I] [Discord] Deleted %d old '%s' post(s) for user '%s' because they contained matching items.", deletedCount, postType, characterName)
		}
	}
}

// createSingleTradingPost now uses the helper for cleanup.
func createSingleTradingPost(authorName, originalMessage, postType string, items []GeminiTradeItem) (int64, error) {
	characterName := sanitizeString(authorName, nameSanitizer)
	if strings.TrimSpace(characterName) == "" {
		return 0, fmt.Errorf("author name is empty after sanitization")
	}

	title := fmt.Sprintf("%s items via Discord", strings.Title(postType))
	discordContact := fmt.Sprintf("Discord: %s", authorName)

	// Generate token hash
	token, err := generateSecretToken(16)
	if err != nil {
		return 0, fmt.Errorf("could not generate security token: %w", err)
	}
	tokenHash, err := bcrypt.GenerateFromPassword([]byte(token), bcrypt.DefaultCost)
	if err != nil {
		return 0, fmt.Errorf("could not hash token: %w", err)
	}

	tx, err := db.Begin()
	if err != nil {
		return 0, fmt.Errorf("failed to start database transaction: %w", err)
	}
	defer tx.Rollback()

	// --- Refactored Part ---
	// 1. Find and delete old posts within the transaction
	var itemNames []string
	var itemParams []interface{}
	for _, item := range items {
		itemName := sanitizeString(item.Name, itemSanitizer)
		if strings.TrimSpace(itemName) != "" {
			itemNames = append(itemNames, itemName)
			itemParams = append(itemParams, itemName)
		}
	}
	findAndDeleteOldPosts(tx, characterName, discordContact, postType, itemNames, itemParams)
	// --- End Refactored Part ---

	// 2. Insert the new main post
	res, err := tx.Exec(`INSERT INTO trading_posts (title, post_type, character_name, contact_info, notes, created_at, edit_token_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?)`,
		title, postType, characterName, discordContact,
		originalMessage, time.Now().Format(time.RFC3339), string(tokenHash),
	)
	if err != nil {
		return 0, fmt.Errorf("failed to save post from discord: %w", err)
	}
	postID, _ := res.LastInsertId()

	// 3. Prepare and insert all items
	stmt, err := tx.Prepare("INSERT INTO trading_post_items (post_id, item_name, item_id, quantity, price_zeny, price_rmt, payment_methods, refinement, slots, card1, card2, card3, card4) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		return 0, fmt.Errorf("database preparation failed for discord post items: %w", err)
	}
	defer stmt.Close()

	for _, item := range items {
		itemName := sanitizeString(item.Name, itemSanitizer)
		if strings.TrimSpace(itemName) == "" {
			continue
		}

		itemID, findErr := findItemIDByName(itemName, true, item.Slots)
		if findErr != nil {
			log.Printf("[W] [Discord] Error finding item ID for '%s': %v. Proceeding without ID.", itemName, findErr)
		}

		paymentMethods := "zeny"
		if item.PaymentMethods == "rmt" || item.PaymentMethods == "both" {
			paymentMethods = item.PaymentMethods
		}

		card1 := sql.NullString{String: item.Card1, Valid: item.Card1 != ""}
		card2 := sql.NullString{String: item.Card2, Valid: item.Card2 != ""}
		card3 := sql.NullString{String: item.Card3, Valid: item.Card3 != ""}
		card4 := sql.NullString{String: item.Card4, Valid: item.Card4 != ""}

		if _, err := stmt.Exec(postID, itemName, itemID, item.Quantity, item.PriceZeny, item.PriceRMT, paymentMethods, item.Refinement, item.Slots, card1, card2, card3, card4); err != nil {
			return 0, fmt.Errorf("failed to save item '%s' for discord post: %w", itemName, err)
		}
	}

	// 4. Commit
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to finalize discord post transaction: %w", err)
	}

	return postID, nil
}

func CreateTradingPostFromDiscord(authorName string, originalMessage string, tradeData *GeminiTradeResult) ([]int64, error) {
	var buyingItems []GeminiTradeItem
	var sellingItems []GeminiTradeItem
	var postIDs []int64
	var finalError error

	for _, item := range tradeData.Items {
		if item.Action == "buying" {
			buyingItems = append(buyingItems, item)
		} else {

			sellingItems = append(sellingItems, item)
		}
	}

	if len(buyingItems) > 0 {
		postID, err := createSingleTradingPost(authorName, originalMessage, "buying", buyingItems)
		if err != nil {
			log.Printf("[E] [Discord] Failed to create 'buying' post for '%s': %v", authorName, err)
			finalError = err
		} else {
			postIDs = append(postIDs, postID)
		}
	}

	if len(sellingItems) > 0 {
		postID, err := createSingleTradingPost(authorName, originalMessage, "selling", sellingItems)
		if err != nil {
			log.Printf("[E] [Discord] Failed to create 'selling' post for '%s': %v", authorName, err)
			finalError = err
		} else {
			postIDs = append(postIDs, postID)
		}
	}

	return postIDs, finalError
}
