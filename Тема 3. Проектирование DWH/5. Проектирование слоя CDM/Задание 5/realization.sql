ALTER TABLE cdm.dm_settlement_report 
    ADD CONSTRAINT dm_settlement_report_unique UNIQUE(restaurant_id, settlement_date);