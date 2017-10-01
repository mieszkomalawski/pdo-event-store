CREATE TABLE `projections` (
  `no` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  `name` VARCHAR(150) NOT NULL,
  `position` JSON,
  `state` JSON,
  `status` VARCHAR(28) NOT NULL,
  `locked_until` CHAR(26),
  UNIQUE (`name`)
);
