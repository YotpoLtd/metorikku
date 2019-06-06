UPDATE customers SET first_name='Anne Maries' WHERE id=1004;
UPDATE customers SET first_name='Anne Mariess' WHERE id=1004;
UPDATE customers SET first_name='Anne Mariesss' WHERE id=1004;
UPDATE customers SET first_name='Anne Mariessss' WHERE id=1004;
UPDATE customers SET first_name='Anne Mariesssss' WHERE id=1004;

SET FOREIGN_KEY_CHECKS=0; -- to disable the foreign key check before delete
DELETE FROM customers WHERE id=1003;
SET FOREIGN_KEY_CHECKS=1; -- to re-enable the foreign key check before delete

SELECT * FROM customers;

ALTER TABLE customers ADD last_updated TIMESTAMP;
UPDATE customers SET last_updated='2019-05-05 18:30:00' WHERE id=1001;
