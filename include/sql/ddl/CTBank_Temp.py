QUERY = """
CREATE TABLE IF NOT EXISTS {FULL_TABLE_NAME} (
  `BankId` int(11) DEFAULT NULL,
  `BankCode` int(11) DEFAULT NULL,
  `BankName` varchar(64) DEFAULT NULL,
  `Abbreviation` varchar(50) DEFAULT NULL,
  `IsActive` varchar(50) DEFAULT NULL,
  `IsDeleted` int(11) DEFAULT NULL,
  `CreatedBy` int(11) DEFAULT NULL,
  `CreatedDate` varchar(50) DEFAULT NULL,
  `UpdateBy` int(11) DEFAULT NULL,
  `UpdateDate` varchar(50) DEFAULT NULL
) ;
"""
