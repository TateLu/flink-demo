

--
-- Table structure for table `demo`
--

DROP TABLE IF EXISTS `demo`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `demo` (
  `id` varchar(100) COLLATE utf8mb4_general_ci NOT NULL,
  `name` varchar(100) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `start_time` datetime DEFAULT NULL,
  `sales` int DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `demo`
--

LOCK TABLES `demo` WRITE;
/*!40000 ALTER TABLE `demo` DISABLE KEYS */;
INSERT INTO `demo` VALUES ('1','David666','2024-02-02 10:54:52',1),('11','David','2024-02-02 10:55:32',7),('123','333','2024-02-02 10:54:52',2),('1234','sdfa',NULL,NULL),('2','Bob','2024-02-02 10:55:02',3),('3','Bob','2024-02-02 10:55:12',4),('333','333','2024-02-02 10:54:52',5),('4','David','2024-02-02 10:55:22',6),('444','adfa',NULL,NULL),('888','sdfa',NULL,NULL),('999','David',NULL,NULL);
/*!40000 ALTER TABLE `demo` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `demo_sum`
--

DROP TABLE IF EXISTS `demo_sum`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `demo_sum` (
  `name` varchar(100) COLLATE utf8mb4_general_ci NOT NULL,
  `total` int DEFAULT NULL,
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

