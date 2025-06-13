-- db_init/01_schema.sql
CREATE DATABASE IF NOT EXISTS olympic_dataset;
USE olympic_dataset;

-- Повна схема для athlete_bio
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

-- Повна схема для athlete_event_results
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

-- Схема для фінальної таблиці з агрегатами
CREATE TABLE IF NOT EXISTS `sporotikov_athlete_enriched_agg` (
    `sport` VARCHAR(255),
    `medal` VARCHAR(50),
    `sex` VARCHAR(10),
    `country_noc` VARCHAR(10),
    `avg_height` DOUBLE,
    `avg_weight` DOUBLE,
    `timestamp` TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;