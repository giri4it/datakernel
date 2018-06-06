CREATE TABLE `ot_revisions_backup` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `order` bigint NOT NULL,
  `snapshot` longtext,
  `timestamp` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
