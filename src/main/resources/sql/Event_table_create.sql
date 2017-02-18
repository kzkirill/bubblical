CREATE TABLE `bubbling_dev1`.`Events` (
  `time` DATETIME NOT NULL,
  `userID` VARCHAR(45) NOT NULL,
  `action` VARCHAR(45) NULL,
  `duration` DOUBLE NULL,
  INDEX `userid` (`userID` ASC));
