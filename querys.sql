Select all Google symbols using exchange symbol
SELECT a.google_symbol, b.google_code, concat(b.google_code, ":", a.google_symbol) as google_
FROM renaissance.instrument as a inner join renaissance.exchange as b on a.exchange_id=b.id 
where b.google_code = "ETR";


