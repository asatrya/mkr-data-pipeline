-- MySQL dump 10.13  Distrib 5.7.28, for Linux (x86_64)
--
-- Host: 127.0.0.1    Database: transaction_dw
-- ------------------------------------------------------
-- Server version	5.6.46

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `accountNumberDim`
--

DROP TABLE IF EXISTS `accountNumberDim`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `accountNumberDim` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `account_number` varchar(256) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `accountNumberDim`
--

LOCK TABLES `accountNumberDim` WRITE;
/*!40000 ALTER TABLE `accountNumberDim` DISABLE KEYS */;
/*!40000 ALTER TABLE `accountNumberDim` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dimDate`
--

DROP TABLE IF EXISTS `dimDate`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dimDate` (
  `date_key` int(11) NOT NULL,
  `date` date NOT NULL,
  `year` smallint(6) NOT NULL,
  `quarter` tinyint(4) NOT NULL,
  `month` tinyint(4) NOT NULL,
  `day` tinyint(4) NOT NULL,
  `week` tinyint(4) NOT NULL,
  `is_weekend` tinyint(1) DEFAULT NULL,
  `is_holiday` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`date_key`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dimDate`
--

LOCK TABLES `dimDate` WRITE;
/*!40000 ALTER TABLE `dimDate` DISABLE KEYS */;
/*!40000 ALTER TABLE `dimDate` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `trxAmountFact`
--

DROP TABLE IF EXISTS `trxAmountFact`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `trxAmountFact` (
  `date` int(11) NOT NULL,
  `account_number_id` int(11) NOT NULL,
  `withdrawal_amount` decimal(15,2) DEFAULT NULL,
  `deposit_amount` decimal(15,2) DEFAULT NULL,
  KEY `trxAmountFact_FK` (`date`),
  KEY `trxAmountFact_FK_1` (`account_number_id`),
  CONSTRAINT `trxAmountFact_FK` FOREIGN KEY (`date`) REFERENCES `dimDate` (`date_key`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `trxAmountFact_FK_1` FOREIGN KEY (`account_number_id`) REFERENCES `accountNumberDim` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `trxAmountFact`
--

LOCK TABLES `trxAmountFact` WRITE;
/*!40000 ALTER TABLE `trxAmountFact` DISABLE KEYS */;
/*!40000 ALTER TABLE `trxAmountFact` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2019-12-01 18:03:42
