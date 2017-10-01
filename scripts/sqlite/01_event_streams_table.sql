CREATE TABLE `event_streams` (
  `no` INTEGER  PRIMARY KEY AUTOINCREMENT NOT NULL,
  `real_stream_name` VARCHAR(150) NOT NULL,
  `stream_name` CHAR(41) NOT NULL,
  `metadata` JSON,
  `category` VARCHAR(150),
  UNIQUE  (`real_stream_name`)
) ;
