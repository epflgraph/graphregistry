
CREATE TABLE IF NOT EXISTS user (
  id varchar(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  name text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  email varchar(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  emailVerified tinyint(1) NOT NULL,
  image text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  createdAt datetime(3) NOT NULL,
  updatedAt datetime(3) NOT NULL,
  given_name varchar(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  family_name varchar(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  gaspar varchar(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  uniqueid varchar(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY user_email_key (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS account (
  id varchar(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  accountId text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  providerId text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  userId varchar(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  accessToken text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  refreshToken text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  idToken text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  accessTokenExpiresAt datetime(3) DEFAULT NULL,
  refreshTokenExpiresAt datetime(3) DEFAULT NULL,
  scope text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  password text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  createdAt datetime(3) NOT NULL,
  updatedAt datetime(3) NOT NULL,
  PRIMARY KEY (id),
  KEY account_userId_fkey (userId),
  CONSTRAINT account_userId_fkey FOREIGN KEY (userId) REFERENCES user (id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS chat_conversation (
  id int NOT NULL AUTO_INCREMENT,
  chat_id varchar(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  user_id varchar(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  model varchar(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  parent_message_id varchar(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  messages json NOT NULL,
  message_id varchar(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  message_text text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  response text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  created_at datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at datetime(3) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY chat_conversation_message_id_key (message_id),
  KEY chat_conversation_parent_message_id_fkey (parent_message_id),
  KEY chat_conversation_user_id_fkey (user_id),
  CONSTRAINT chat_conversation_parent_message_id_fkey FOREIGN KEY (parent_message_id) REFERENCES chat_conversation (message_id) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT chat_conversation_user_id_fkey FOREIGN KEY (user_id) REFERENCES user (id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=1225 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS _prisma_migrations (
  id varchar(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  checksum varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  finished_at datetime(3) DEFAULT NULL,
  migration_name varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  logs text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  rolled_back_at datetime(3) DEFAULT NULL,
  started_at datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  applied_steps_count int unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS session (
  id varchar(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  expiresAt datetime(3) NOT NULL,
  token varchar(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  createdAt datetime(3) NOT NULL,
  updatedAt datetime(3) NOT NULL,
  ipAddress text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  userAgent text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  userId varchar(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY session_token_key (token),
  KEY session_userId_fkey (userId),
  CONSTRAINT session_userId_fkey FOREIGN KEY (userId) REFERENCES user (id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS sessions (
  session_id varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  expires int unsigned NOT NULL,
  data mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
  PRIMARY KEY (session_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS verification (
  id varchar(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  identifier text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  value text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  expiresAt datetime(3) NOT NULL,
  createdAt datetime(3) DEFAULT NULL,
  updatedAt datetime(3) DEFAULT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
