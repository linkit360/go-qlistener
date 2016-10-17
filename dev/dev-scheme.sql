CREATE TABLE tbl_retry_transaction
(
  "C1" INTEGER,
  "C2" TEXT,
  "C3" TEXT,
  "C4" TEXT,
  "C5" NUMERIC,
  "C6" TEXT,
  "C7" INTEGER,
  "C8" TEXT,
  "C9" NUMERIC,
  "C10" TEXT,
  "C11" INTEGER,
  "C12" TEXT,
  "C13" TEXT,
  "C14" TEXT
);
CREATE TABLE xmp_billing_partner_operators
(
  id INTEGER PRIMARY KEY NOT NULL,
  id_bp INTEGER,
  operator_code INTEGER,
  revenue_sharing INTEGER,
  created_at TIMESTAMP
);
CREATE TABLE xmp_billing_partners
(
  id INTEGER PRIMARY KEY NOT NULL,
  name VARCHAR(256),
  country_code INTEGER,
  bp_operators VARCHAR(256),
  description VARCHAR(512),
  revenue_sharing VARCHAR(256),
  created_at TIMESTAMP DEFAULT now() NOT NULL
);
CREATE TABLE xmp_campaign_ratio_counter
(
  id_campaign INTEGER,
  counter INTEGER,
  id serial
);
CREATE INDEX index_for_xmp_campaign_ratio_counter_field_id_campaign ON xmp_campaign_ratio_counter (id_campaign);
CREATE TABLE xmp_campaigns
(
  id INTEGER PRIMARY KEY NOT NULL,
  link VARCHAR(512),
  name VARCHAR(128) NOT NULL,
  id_publisher INTEGER NOT NULL,
  priority INTEGER,
  status INTEGER,
  page_welcome VARCHAR(32),
  page_msisdn VARCHAR(32),
  page_pin VARCHAR(32),
  page_thank_you VARCHAR(32),
  page_error VARCHAR(32),
  page_banner VARCHAR(32),
  description VARCHAR(250),
  ak_ratio INTEGER,
  capping_days INTEGER,
  capping_max_revenue INTEGER,
  blacklist INTEGER,
  whitelist INTEGER,
  created_at TIMESTAMP DEFAULT now(),
  service_id_1 INTEGER,
  service_id_2 INTEGER,
  service_id_3 INTEGER,
  ak INTEGER,
  ak_time_based TIMESTAMP,
  capping INTEGER,
  capping_currency INTEGER,
  cpa INTEGER,
  hash VARCHAR(32),
  type INTEGER DEFAULT 1 NOT NULL,
  created_by INTEGER
);
CREATE TABLE xmp_cheese_dynamic_url_log
(
  id serial,
  url VARCHAR(512),
  id_service INTEGER,
  created_at TIMESTAMP DEFAULT now()
);
CREATE TABLE xmp_content
(
  id INTEGER PRIMARY KEY NOT NULL,
  content_name VARCHAR(256),
  id_category INTEGER,
  id_sub_category INTEGER,
  publisher_name VARCHAR(256),
  id_platform INTEGER,
  id_uploader INTEGER,
  id_publisher INTEGER,
  status INTEGER,
  created_at TIMESTAMP DEFAULT now() NOT NULL,
  name VARCHAR(256) DEFAULT ''::character varying,
  object VARCHAR(32) DEFAULT ''::character varying,
  id_content_provider INTEGER NOT NULL
);
CREATE TABLE xmp_content_blacklist
(
  id INTEGER PRIMARY KEY NOT NULL,
  category VARCHAR(32),
  id_unit INTEGER,
  id_country INTEGER
);
CREATE TABLE xmp_content_category
(
  id INTEGER PRIMARY KEY NOT NULL,
  name VARCHAR(64),
  icon VARCHAR(64)
);
CREATE TABLE xmp_content_links
(
  id INTEGER PRIMARY KEY NOT NULL,
  id_content INTEGER,
  link VARCHAR(512),
  created_at TIMESTAMP DEFAULT now(),
  ttl_hours INTEGER,
  id_subscription INTEGER,
  counter INTEGER,
  status INTEGER
);
CREATE UNIQUE INDEX xmp_content_links_id_uindex ON xmp_content_links (id);
CREATE TABLE xmp_content_platforms
(
  id INTEGER PRIMARY KEY NOT NULL,
  name VARCHAR(64)
);
CREATE TABLE xmp_content_providers
(
  id INTEGER PRIMARY KEY NOT NULL,
  name VARCHAR(32) NOT NULL
);
CREATE TABLE xmp_content_publishers
(
  id INTEGER PRIMARY KEY NOT NULL,
  name VARCHAR(32) NOT NULL,
  description VARCHAR(512)
);
CREATE TABLE xmp_content_sub_category
(
  id INTEGER PRIMARY KEY NOT NULL,
  name VARCHAR(64),
  description VARCHAR(512)
);
CREATE TABLE xmp_conversion_report
(
  id serial,
  id_report VARCHAR(32),
  lp INTEGER,
  msisdn INTEGER,
  pin INTEGER,
  subscribed INTEGER,
  first_charge INTEGER,
  subscribers_rate DOUBLE PRECISION,
  msisdn_rate DOUBLE PRECISION,
  pin_rate DOUBLE PRECISION,
  tariffication_rate DOUBLE PRECISION,
  date DATE,
  link VARCHAR(64),
  thank_you INTEGER,
  unsubscribed INTEGER,
  uniq_subscribed INTEGER,
  country_code VARCHAR(8),
  operator_code VARCHAR(8)
);
CREATE TABLE xmp_countries
(
  id INTEGER PRIMARY KEY NOT NULL,
  name VARCHAR(250),
  code INTEGER,
  status INTEGER,
  iso VARCHAR(32),
  priority INTEGER
);
CREATE TABLE xmp_cqrs_log
(
  id serial,
  service_name VARCHAR(64),
  service_method VARCHAR(64),
  data VARCHAR(256),
  created_at TIMESTAMP DEFAULT now()
);
CREATE TABLE xmp_currency
(
  id INTEGER PRIMARY KEY NOT NULL,
  code VARCHAR(3)
);
CREATE TABLE xmp_inject_jobs
(
  id serial,
  name VARCHAR(64),
  description VARCHAR(256),
  wording VARCHAR(256),
  country_code INTEGER,
  operator_code INTEGER,
  arpu INTEGER,
  status_msisdn INTEGER,
  id_platform INTEGER,
  total_msisdn INTEGER,
  status_job INTEGER,
  created_at TIMESTAMP DEFAULT now()
);
CREATE TABLE xmp_mobilink_queue
(
  id serial,
  created_at TIMESTAMP DEFAULT now(),
  campaign VARCHAR(64),
  headers JSON
);
CREATE TABLE xmp_mobilink_revenue_report
(
  id serial,
  created_at TIMESTAMP DEFAULT now(),
  total_mo INTEGER,
  total_mo_uniq INTEGER,
  total_mo_uniq_success_charge INTEGER,
  total_mo_success_charge INTEGER,
  total_mo_failed_charge INTEGER,
  total_mo_revenue INTEGER,
  total_retry INTEGER,
  total_uniq_retry INTEGER,
  total_success_retry INTEGER,
  total_failed_retry INTEGER,
  total_mo_success_rate DOUBLE PRECISION,
  total_mo_uniq_success_rate DOUBLE PRECISION,
  total_retry_success_rate DOUBLE PRECISION,
  id_report INTEGER,
  report_date DATE,
  total_lp_hits INTEGER,
  total_mo_uniq_30_days INTEGER,
  total_retry_revenue INTEGER,
  total_revenue INTEGER
);
CREATE TABLE xmp_mobilink_transactions
(
  id serial,
  created_at TIMESTAMP DEFAULT now(),
  campaign VARCHAR(64),
  msisdn VARCHAR(16),
  status INTEGER,
  id_task INTEGER,
  type INTEGER
);
CREATE TABLE xmp_msisdn_blacklist
(
  id serial,
  msisdn VARCHAR(32)
);
CREATE TABLE xmp_msisdn_whitelist
(
  id INTEGER,
  msisdn VARCHAR(32)
);
CREATE TABLE xmp_operator_ip
(
  id INTEGER PRIMARY KEY NOT NULL,
  ip_from VARCHAR(32),
  ip_to VARCHAR(32),
  operator_code INTEGER
);
CREATE TABLE xmp_operator_msisdn_prefix
(
  id INTEGER PRIMARY KEY NOT NULL,
  operator_code INTEGER,
  prefix VARCHAR(8)
);
CREATE TABLE xmp_operators
(
  id INTEGER PRIMARY KEY NOT NULL,
  name VARCHAR(64),
  country_code INTEGER,
  isp VARCHAR(250),
  msisdn_prefix VARCHAR(128),
  mcc VARCHAR(8),
  mnc VARCHAR(8),
  created_at TIMESTAMP DEFAULT now(),
  status INTEGER DEFAULT 1,
  code INTEGER
);
CREATE TABLE xmp_payment_type
(
  id INTEGER PRIMARY KEY NOT NULL,
  name VARCHAR(32),
  description VARCHAR(64)
);
CREATE TABLE xmp_publishers
(
  id INTEGER PRIMARY KEY NOT NULL,
  name VARCHAR(256),
  contact_person VARCHAR(256),
  created_at TIMESTAMP DEFAULT now() NOT NULL,
  publisher_code INTEGER,
  status INTEGER
);
CREATE TABLE xmp_publishers_attributes
(
  id serial,
  country_code INTEGER,
  operator_code INTEGER,
  id_service INTEGER,
  price INTEGER,
  id_currency INTEGER,
  id_publisher INTEGER,
  callback_url VARCHAR(512),
  publisher_code INTEGER,
  id_campaign INTEGER,
  cpa_ratio DOUBLE PRECISION,
  status INTEGER
);
CREATE TABLE xmp_revenue_report
(
  id serial,
  id_report INTEGER,
  date DATE,
  total_subs INTEGER,
  new_subs INTEGER,
  revenue INTEGER,
  country_code INTEGER,
  operator_code INTEGER,
  id_service INTEGER,
  transactions_count INTEGER,
  unsuccess_transactions_count INTEGER,
  unsubscribed_count INTEGER,
  success_revenue_mobilink INTEGER,
  success_charged_mobilink INTEGER,
  mo_new_mobilink INTEGER,
  mo_unique_mobilink INTEGER,
  total_failed_hits_mobilink INTEGER,
  total_retry_hits_mobilink INTEGER,
  unique_retry_hits_mobilink INTEGER
);
CREATE TABLE xmp_roles
(
  id serial,
  role_name VARCHAR(128),
  created_at TIMESTAMP DEFAULT now(),
  created_by INTEGER
);
CREATE TABLE xmp_sections
(
  id serial,
  section_name VARCHAR(128)
);
CREATE TABLE xmp_service_content
(
  id INTEGER PRIMARY KEY NOT NULL,
  id_service INTEGER,
  id_content INTEGER,
  status INTEGER
);
CREATE TABLE xmp_service_country_settings
(
  id INTEGER PRIMARY KEY NOT NULL,
  country_code INTEGER,
  operator_code INTEGER,
  id_service INTEGER
);
CREATE TABLE xmp_services
(
  id INTEGER PRIMARY KEY NOT NULL,
  name VARCHAR(32) NOT NULL,
  description VARCHAR(32),
  keyword VARCHAR(32),
  url VARCHAR(128),
  price DOUBLE PRECISION,
  id_payment_type INTEGER,
  id_subscription_type INTEGER,
  retry_days INTEGER,
  wording TEXT,
  status INTEGER,
  id_currency INTEGER,
  created_at TIMESTAMP DEFAULT now() NOT NULL,
  channel_sms INTEGER,
  channel_wap INTEGER,
  channel_web INTEGER,
  start_date TIMESTAMP,
  price_option VARCHAR(32),
  link VARCHAR(128),
  pull_msisdn_ttr INTEGER,
  pull_retry_delay INTEGER,
  sms_send INTEGER
);
CREATE TABLE xmp_subscribers
(
  id INTEGER PRIMARY KEY NOT NULL,
  msisdn VARCHAR(32) NOT NULL,
  created_at TIMESTAMP(0) DEFAULT now(),
  id_campaign INTEGER,
  operator_code INTEGER,
  status INTEGER
);
CREATE UNIQUE INDEX xmp_subscribers_msisdn_uindex ON xmp_subscribers (msisdn);
CREATE TABLE xmp_subscription_type
(
  id INTEGER PRIMARY KEY NOT NULL,
  name VARCHAR(32),
  description VARCHAR(64)
);
CREATE TABLE xmp_subscriptions
(
  last_success_date TIMESTAMP DEFAULT now(),
  id_service INTEGER,
  country_code INTEGER,
  id_subscriber INTEGER,
  created_at TIMESTAMP DEFAULT now(),
  msisdn VARCHAR(32),
  status INTEGER,
  operator_code INTEGER,
  last_content_id INTEGER,
  id INTEGER PRIMARY KEY NOT NULL,
  id_campaign INTEGER
);
CREATE UNIQUE INDEX xmp_subscriptions_id_uindex ON xmp_subscriptions (id);
CREATE TABLE xmp_subscriptions_active
(
  id_service INTEGER,
  country_code INTEGER,
  created_at TIMESTAMP DEFAULT now(),
  msisdn VARCHAR(32),
  status INTEGER,
  operator_code INTEGER,
  id serial
);
CREATE INDEX index_xmp_subscriptions_active_id_service ON xmp_subscriptions_active (id_service);
CREATE INDEX index_xmp_subscriptions_id_service ON xmp_subscriptions_active (id_service);
CREATE INDEX index_xmp_subscriptions_active_country_code ON xmp_subscriptions_active (country_code);
CREATE INDEX index_xmp_subscriptions_country_code ON xmp_subscriptions_active (country_code);
CREATE INDEX index_xmp_subscriptions_active_on_created_at ON xmp_subscriptions_active (created_at);
CREATE INDEX index_xmp_subscriptions_created_at ON xmp_subscriptions_active (created_at);
CREATE INDEX index_xmp_subscriptions_active_operator_code ON xmp_subscriptions_active (operator_code);
CREATE INDEX index_xmp_subscriptions_operator_code ON xmp_subscriptions_active (operator_code);
CREATE TABLE xmp_transaction_types
(
  id serial,
  name VARCHAR(64)
);
CREATE TABLE xmp_transactions
(
  id INTEGER PRIMARY KEY NOT NULL,
  created_at TIMESTAMP DEFAULT now(),
  tran_type INTEGER,
  msisdn VARCHAR(32),
  country_code INTEGER,
  id_service INTEGER,
  status INTEGER,
  operator_code INTEGER,
  id_subscription INTEGER,
  id_content INTEGER
);
CREATE UNIQUE INDEX xmp_transactions_id_uindex ON xmp_transactions (id);
CREATE INDEX index_on_created_at ON xmp_transactions (created_at);
CREATE INDEX index_on_tran_type ON xmp_transactions (tran_type);
CREATE INDEX index_on_country_code ON xmp_transactions (country_code);
CREATE INDEX index_on_id_service ON xmp_transactions (id_service);
CREATE INDEX index_on_operator_code ON xmp_transactions (operator_code);
CREATE TABLE xmp_transactions_dr
(
  id serial,
  created_at TIMESTAMP,
  tran_type INTEGER,
  msisdn VARCHAR(32),
  country_code INTEGER,
  id_service INTEGER,
  status INTEGER,
  operator_code INTEGER
);
CREATE INDEX index_xmp_transactions_dr_on_created_at ON xmp_transactions_dr (created_at);
CREATE INDEX index_xmp_transactions_dr_on_tran_type ON xmp_transactions_dr (tran_type);
CREATE INDEX index_xmp_transactions_dr_on_country_code ON xmp_transactions_dr (country_code);
CREATE INDEX index_xmp_transactions_dr_on_id_service ON xmp_transactions_dr (id_service);
CREATE INDEX index_xmp_transactions_dr_on_operator_code ON xmp_transactions_dr (operator_code);
CREATE TABLE xmp_transactions_dr_test
(
  id serial,
  msisdn VARCHAR(32),
  id_service INTEGER,
  created_at TIMESTAMP
);
CREATE TABLE xmp_uniq_url
(
  id serial,
  file_src VARCHAR(128),
  file_uniq_linq VARCHAR(256),
  created_at TIMESTAMP DEFAULT now(),
  expired INTEGER
);
CREATE TABLE xmp_user_activity_logs
(
  id INTEGER PRIMARY KEY NOT NULL,
  created_at TIMESTAMP DEFAULT now(),
  id_xmp_user INTEGER,
  activity VARCHAR(64),
  id_record INTEGER,
  ip_user VARCHAR(32)
);
CREATE TABLE xmp_user_data_reports
(
  id serial,
  created_at TIMESTAMP DEFAULT now(),
  created_by INTEGER,
  report_date DATE,
  report_type VARCHAR(32),
  report_status INTEGER,
  file_link VARCHAR(128),
  id_report INTEGER,
  updated_at TIMESTAMP,
  campaign VARCHAR(64)
);
CREATE TABLE xmp_user_role_permissions
(
  id serial,
  id_role INTEGER,
  id_section INTEGER,
  action_name VARCHAR(256),
  permission_name VARCHAR(256)
);
CREATE TABLE xmp_user_roles
(
  id serial,
  id_user INTEGER,
  id_role INTEGER
);
CREATE TABLE xmp_user_roles_orig
(
  id serial,
  id_user INTEGER,
  id_role INTEGER
);
CREATE TABLE xmp_users_types
(
  id INTEGER PRIMARY KEY NOT NULL,
  name VARCHAR(32),
  description VARCHAR(256)
);

CREATE TABLE xmp_users
(
  id INTEGER PRIMARY KEY NOT NULL,
  username VARCHAR(32),
  password VARCHAR(32),
  email VARCHAR(64),
  type INTEGER,
  active INTEGER,
  CONSTRAINT xmp_users_type_fkey FOREIGN KEY (type) REFERENCES xmp_users_types (id)
);
CREATE TABLE xmp_users_info
(
  id serial,
  id_xmp_user INTEGER,
  user_position VARCHAR(64),
  user_about VARCHAR(512),
  user_phone VARCHAR(32),
  user_facebook VARCHAR(128),
  user_linkedin VARCHAR(128),
  id_user_group INTEGER,
  user_fullname VARCHAR(128),
  user_avatar VARCHAR(256),
  CONSTRAINT xmp_users_info_id_xmp_user_fkey FOREIGN KEY (id_xmp_user) REFERENCES xmp_users (id)
);
CREATE TABLE xmp_users_transaction_type
(
  id serial,
  name VARCHAR(32)
);
CREATE TABLE xmp_users_transactions
(
  id serial,
  id_xmp_user INTEGER,
  id_xmp_users_transaction_type INTEGER,
  created_at TIMESTAMP DEFAULT now()
);
