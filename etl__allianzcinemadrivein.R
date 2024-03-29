library(RMariaDB)
library(DBI)
library(tidyverse)

SQL__SOLD_TICKETS <<-
"SELECT DISTINCT
                cc.firstName,
                cc.lastName,
                asp.amount as ticketprice,
                ticket.id                                                                     as ticketId,
                CAST(ticket.row as CHAR(10))                                                  as ticketRow,
                CAST(ticket.seatInRow as CHAR(10))                                            as ticketSeatInRow,
                ae_show.startDate,
                ae_show.no                                                                    as showNo,
                ae_show.id                                                                    as showId,
                show_translation.title                                                        as showTitle,
                event_translation.title                                                       as eventTitle,
                organizer.name                                                                as organizerName,
                contingent.name                                                               as contingentName,
                seatmap_section.dtype,
                seatmap_section_translation.title                                             as seatmanSectionTitleName,
                ticket.code,
                ticket.currentPlace,
                promotion.type                                                                AS promotionType,
                ao.channel,
                ao.orderDate                                                                  as orderDate,
                ROUND(order_item.singleAmount / 100, 2)                                       AS totalAmount,
                ROUND(aoia.amount / 100, 2)                                                   as adjustment,
                att.title                                                                     as tariffTitle,
                CASE WHEN ticket.firstScanned IS NULL THEN 0 ELSE 1 END                       AS scanned,
                IF(ticket.currentPlace = 'assigned' OR ticket.currentPlace = 'shipped', 1, 0) as ticketsTotal,
                IF(ticket.currentPlace = 'assigned', 1, 0)                                    as assigned,
                IF(ticket.currentPlace = 'shipped', 1, 0)                                     as shipped,
                IF(ticket.currentPlace = 'canceled', 1, 0)				      as canceled,
		IF(ticket.currentPlace = 'available', 1, 0)                                   as available,
                IF(seatmap_section_translation.title = 'VIP Parkplatz' AND
                   (ticket.currentPlace = 'assigned' OR ticket.currentPlace = 'shipped'), 1,
                   0)                                                                         as `VIP Parkplatz verkauft`,
                IF(seatmap_section_translation.title = 'VIP Parkplatz' AND ticket.currentPlace = 'available', 1,
                   0)                                                                         as `VIP Parkplatz verfügbar`,
                IF(seatmap_section_translation.title = 'vor Ort zugewiesener Parkplatz' AND
                   (ticket.currentPlace = 'assigned' OR ticket.currentPlace = 'shipped'), 1,
                   0)                                                                         as `vor Ort zugewiesener Parkplatz verkauft`,
                IF(seatmap_section_translation.title = 'vor Ort zugewiesener Parkplatz' AND ticket.currentPlace = 'available', 1,
                   0)                                                                         as `vor Ort zugewiesener Parkplatz verfügbar`,
                GROUP_CONCAT(promotion.name)                                                  as promotionName,
                GROUP_CONCAT(account.name)                                                    as accountName,
                ao.lastDatatransUppTransactionId                                              as lastDatatransUppTransactionId
FROM app_tickets AS ticket
         LEFT JOIN app_order_ticket_references AS ticket_reference ON ticket_reference.ticketId = ticket.id
         LEFT JOIN app_order_items AS order_item ON order_item.no = ticket_reference.orderItemId
         LEFT JOIN app_show_contingents AS show_contingent ON show_contingent.id = ticket.showContingentId
         LEFT JOIN app_contingents AS contingent ON show_contingent.contingentId = contingent.id
         LEFT JOIN app_show_seatmap_sections AS show_seatmap_section
                   ON show_seatmap_section.id = ticket.showSeatmapSectionId AND show_seatmap_section.stage = 'live'
         LEFT JOIN app_seatmap_sections AS seatmap_section ON seatmap_section.id = show_seatmap_section.seatmapSectionId
         LEFT JOIN app_seatmap_section_translation AS seatmap_section_translation
                   ON seatmap_section_translation.seatmapSectionId = seatmap_section.no AND
                      seatmap_section_translation.locale = 'de'
         LEFT JOIN app_order_promotion_references AS order_promotion_reference
                   ON order_promotion_reference.orderItemId = order_item.no
         LEFT JOIN app_promotions AS promotion ON promotion.id = order_promotion_reference.promotionId
         LEFT JOIN co_accounts AS account ON account.id = promotion.partnerId
         LEFT JOIN ae_shows AS ae_show ON ae_show.id = ticket.showId AND ae_show.stage = 'live'
         LEFT JOIN ae_show_translations AS show_translation
                   ON show_translation.showId = ae_show.no AND show_translation.locale = 'de'
         LEFT JOIN ae_events AS ae_event ON ae_event.id = ae_show.eventId
         LEFT JOIN ae_event_translations AS event_translation
                   ON event_translation.eventId = ae_event.no AND event_translation.locale = 'de'
         LEFT JOIN co_accounts AS organizer ON organizer.id = ae_event.organizerId
         LEFT JOIN app_orders ao ON order_item.orderId = ao.no
         LEFT JOIN ae_tariffs ae ON ae.id = order_item.tariffId
         LEFT JOIN ae_tariff_translations att ON ae.no = att.tariff_id AND att.locale = 'de'
         LEFT JOIN app_order_item_adjustments aoia ON order_item.no = aoia.orderItemId
         LEFT JOIN app_access_starticket_show_contingents aassc ON aassc.internalId = ae_show.id
         LEFT JOIN co_contacts cc on ao.contactId = cc.id
         LEFT JOIN app_show_prices asp on ae.id = asp.tariffId
WHERE 
	ao.orderDate >= '2021-07-01'
GROUP BY ticket.id;"

MYSQL_CON <<- NULL

mysql_connect <- function() {
  
  if (is.null(MYSQL_CON)) {
    MYSQL_CON <<- dbConnect(
      MariaDB(),
      host = "34.65.250.163",
      user = "allianzcinema",
      password = "EHN8V28k9Uejk2F8rqKwJz",
      dbname = "su_allianzcinema"
    )
  }
}

allianz_getSoldTickets <- function() {

  mysql_connect()
  
  res <- dbGetQuery(MYSQL_CON, SQL__SOLD_TICKETS)
  
  dbDisconnect(MYSQL_CON)
  
  return(res)
}

helper__transform_column_names <- function(df) {
  
  names(df) <- gsub("[[:space:]]", "_", names(df))
  names(df) <- gsub("ä", "ae", names(df))
  names(df) <- gsub("ü", "ue", names(df))
  names(df) <- gsub("ö", "oe", names(df))
  names(df) <- gsub("ß", "ss", names(df))
  
  return(df)
}

convert_numbers <- function(df) {
  df$totalAmount[is.na(df$totalAmount)] <- 0
  df$totalAmount <- as.integer(df$totalAmount)
  df %>% glimpse()  
  return(df)
}
