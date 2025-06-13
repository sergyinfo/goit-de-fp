CREATE DATABASE IF NOT EXISTS olympic_dataset;
USE olympic_dataset;

CREATE TABLE IF NOT EXISTS `athlete_bio` (
  `athlete_id` int DEFAULT NULL,
  `name` text,
  `sex` text,
  `born` text,
  `height` text,
  `weight` text,
  `country` text,
  `country_noc` text,
  `description` text,
  `special_notes` text,
   KEY `athlete_id_idx` (`athlete_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS `athlete_event_results` (
  `edition` text,
  `edition_id` int DEFAULT NULL,
  `country_noc` text,
  `sport` text,
  `event` text,
  `result_id` bigint DEFAULT NULL,
  `athlete` text,
  `athlete_id` int DEFAULT NULL,
  `pos` text,
  `medal` text,
  `isTeamSport` text,
   KEY `athlete_id_idx` (`athlete_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS `sporotikov_athlete_enriched_agg` (
    `sport` VARCHAR(255),
    `medal` VARCHAR(50),
    `sex` VARCHAR(10),
    `country_noc` VARCHAR(10),
    `avg_height` DOUBLE,
    `avg_weight` DOUBLE,
    `timestamp` TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;