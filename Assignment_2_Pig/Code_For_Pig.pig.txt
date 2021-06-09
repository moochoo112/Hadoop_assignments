
-- Get CSVExcelStorage
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage; 

-- Load CSV-file from the orders.csv file
orders = LOAD 'usermaria_devdiplomacyorders.csv' USING CSVExcelStorage() AS
(game_idint,
unit_idint,
unit_orderchararray,
locationchararray,
targetchararray,
target_destchararray,
successint,
reasonint,
turn_numint);

-- Filter on target 'Holland'. This is done so that the group is faster
orders_filtered = FILTER orders BY target == 'Holland';

-- Group by “location” with target Holland
orders_grouped = GROUP orders_filtered BY (location, target);

-- Count how many times Holland was the target from that location
orders_unordered = FOREACH orders_grouped GENERATE group, COUNT(orders_filtered);

-- Make a alphabetic list from all locations from the orders.csv
orders_list = ORDER orders_unordered BY $0 ASC;

-- Print with DUMP-statement
DUMP orders_list;