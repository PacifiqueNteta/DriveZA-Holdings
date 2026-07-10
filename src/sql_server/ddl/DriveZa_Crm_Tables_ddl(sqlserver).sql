
-- Create database
CREATE DATABASE DriveZA;
GO

USE DriveZA;
GO

CREATE SCHEMA crm;
GO


-- 1. crm.customers
CREATE TABLE crm.customers (
    customer_id                 VARCHAR(10)         NOT NULL,
    first_name                  NVARCHAR(50)        NOT NULL,
    last_name                   NVARCHAR(50)        NOT NULL,
    email                       NVARCHAR(100)       NOT NULL,
    phone_number                VARCHAR(20)         NOT NULL,
    id_number                   VARCHAR(20)         NULL,
    passport_number             VARCHAR(20)         NULL,
    nationality                 VARCHAR(50)         NOT NULL,
    date_of_birth               DATE                NOT NULL,
    gender                      VARCHAR(30)         NOT NULL,
    address_line1               NVARCHAR(100)       NOT NULL,
    address_line2               NVARCHAR(50)        NULL,
    city                        NVARCHAR(50)        NOT NULL,
    province                    VARCHAR(50)         NOT NULL,
    postal_code                 VARCHAR(10)         NOT NULL,
    country                     VARCHAR(50)         NOT NULL,
    drivers_licence_number      VARCHAR(20)         NOT NULL,
    licence_issue_date          DATE                NOT NULL,
    licence_expiry_date         DATE                NOT NULL,
    licence_country             VARCHAR(50)         NOT NULL,
    is_corporate_account        BIT                 NOT NULL    DEFAULT 0,
    company_name                NVARCHAR(150)       NULL,
    company_vat_number          VARCHAR(20)         NULL,
    preferred_payment           VARCHAR(30)         NOT NULL,
    loyalty_tier                VARCHAR(20)         NULL,
    loyalty_points              INT                 NOT NULL    DEFAULT 0,
    marketing_opt_in            BIT                 NOT NULL    DEFAULT 0,
    registration_date           DATE                NOT NULL,
    last_login_date             DATE                NULL,
    total_rentals               INT                 NOT NULL    DEFAULT 0,
    lifetime_spend_zar          DECIMAL(14, 2)      NOT NULL    DEFAULT 0.00,
    blacklisted                 BIT                 NOT NULL    DEFAULT 0,
    blacklist_reason            NVARCHAR(200)       NULL,
    created_at                  DATETIME2           NOT NULL,
    updated_at                  DATETIME2           NOT NULL,
    source_system               VARCHAR(30)         NOT NULL,

    CONSTRAINT pk_customers PRIMARY KEY (customer_id)
);

CREATE INDEX ix_customers_updated_at   ON crm.customers (updated_at);
CREATE INDEX ix_customers_email        ON crm.customers (email);
CREATE INDEX ix_customers_loyalty_tier ON crm.customers (loyalty_tier);
GO


-- 2. crm.rentals
CREATE TABLE crm.rentals (
    rental_id                   VARCHAR(10)         NOT NULL,
    customer_id                 VARCHAR(10)         NOT NULL,
    vehicle_id                  VARCHAR(10)         NOT NULL,
    pickup_branch_id            VARCHAR(10)         NOT NULL,
    dropoff_branch_id           VARCHAR(10)         NOT NULL,
    booking_date                DATE                NOT NULL,
    pickup_date                 DATE                NOT NULL,
    scheduled_dropoff_date      DATE                NOT NULL,
    actual_dropoff_date         DATE                NULL,
    rental_days                 SMALLINT            NOT NULL,
    daily_rate_zar              DECIMAL(10, 2)      NOT NULL,
    base_rental_amount_zar      DECIMAL(12, 2)      NOT NULL,
    insurance_amount_zar        DECIMAL(12, 2)      NOT NULL,
    extras_amount_zar           DECIMAL(10, 2)      NOT NULL    DEFAULT 0.00,
    discount_amount_zar         DECIMAL(10, 2)      NOT NULL    DEFAULT 0.00,
    total_amount_zar            DECIMAL(12, 2)      NOT NULL,
    deposit_amount_zar          DECIMAL(10, 2)      NOT NULL,
    deposit_returned            BIT                 NOT NULL    DEFAULT 0,
    payment_method              VARCHAR(30)         NOT NULL,
    payment_status              VARCHAR(20)         NOT NULL,
    insurance_type              VARCHAR(20)         NULL,
    odometer_out_km             INT                 NOT NULL,
    odometer_in_km              INT                 NULL,
    fuel_level_out              VARCHAR(10)         NOT NULL,
    fuel_level_in               VARCHAR(10)         NULL,
    addons                      NVARCHAR(MAX)       NULL,
    special_requests            NVARCHAR(200)       NULL,
    rental_status               VARCHAR(20)         NOT NULL,
    cancellation_reason         NVARCHAR(100)       NULL,
    staff_id                    VARCHAR(10)         NOT NULL,
    booking_channel             VARCHAR(30)         NOT NULL,
    promo_code                  VARCHAR(20)         NULL,
    created_at                  DATETIME2           NOT NULL,
    updated_at                  DATETIME2           NOT NULL,
    source_system               VARCHAR(30)         NOT NULL,

    CONSTRAINT pk_rentals PRIMARY KEY (rental_id)
);

CREATE INDEX ix_rentals_updated_at     ON crm.rentals (updated_at);
CREATE INDEX ix_rentals_customer_id    ON crm.rentals (customer_id);
CREATE INDEX ix_rentals_vehicle_id     ON crm.rentals (vehicle_id);
CREATE INDEX ix_rentals_pickup_date    ON crm.rentals (pickup_date);
CREATE INDEX ix_rentals_rental_status  ON crm.rentals (rental_status);
GO


-- 3. crm.promotions
CREATE TABLE crm.promotions (
    promotion_id                VARCHAR(10)         NOT NULL,
    promo_code                  VARCHAR(20)         NOT NULL,
    promo_name                  NVARCHAR(100)       NOT NULL,
    description                 NVARCHAR(200)       NOT NULL,
    discount_value              DECIMAL(8, 2)       NOT NULL,
    discount_type               VARCHAR(20)         NOT NULL,
    min_rental_days             SMALLINT            NOT NULL    DEFAULT 1,
    min_spend_zar               DECIMAL(10, 2)      NOT NULL    DEFAULT 0.00,
    applicable_categories       VARCHAR(100)        NOT NULL,
    start_date                  DATE                NOT NULL,
    end_date                    DATE                NOT NULL,
    usage_limit                 INT                 NULL,
    times_used                  INT                 NOT NULL    DEFAULT 0,
    is_active                   BIT                 NOT NULL    DEFAULT 1,
    created_at                  DATETIME2           NOT NULL,
    source_system               VARCHAR(30)         NOT NULL,

    CONSTRAINT pk_promotions    PRIMARY KEY (promotion_id),
    CONSTRAINT uq_promo_code    UNIQUE (promo_code)
);

CREATE INDEX ix_promotions_created_at  ON crm.promotions (created_at);
CREATE INDEX ix_promotions_is_active   ON crm.promotions (is_active);
GO

-- 4. crm.reviews
CREATE TABLE crm.reviews (
    review_id                   VARCHAR(10)         NOT NULL,
    rental_id                   VARCHAR(10)         NOT NULL,
    customer_id                 VARCHAR(10)         NOT NULL,
    branch_id                   VARCHAR(10)         NOT NULL,
    overall_rating              TINYINT             NOT NULL,
    vehicle_rating              TINYINT             NOT NULL,
    service_rating              TINYINT             NOT NULL,
    value_rating                TINYINT             NOT NULL,
    cleanliness_rating          TINYINT             NOT NULL,
    review_text                 NVARCHAR(MAX)       NOT NULL,
    review_date                 DATE                NOT NULL,
    review_channel              VARCHAR(30)         NOT NULL,
    response_text               NVARCHAR(MAX)       NULL,
    response_date               DATE                NULL,
    verified_rental             BIT                 NOT NULL    DEFAULT 1,
    created_at                  DATETIME2           NOT NULL,
    source_system               VARCHAR(30)         NOT NULL,

    CONSTRAINT pk_reviews           PRIMARY KEY (review_id),
    CONSTRAINT chk_overall_rating   CHECK (overall_rating      BETWEEN 1 AND 5),
    CONSTRAINT chk_vehicle_rating   CHECK (vehicle_rating      BETWEEN 1 AND 5),
    CONSTRAINT chk_service_rating   CHECK (service_rating      BETWEEN 1 AND 5),
    CONSTRAINT chk_value_rating     CHECK (value_rating        BETWEEN 1 AND 5),
    CONSTRAINT chk_clean_rating     CHECK (cleanliness_rating  BETWEEN 1 AND 5)
);

CREATE INDEX ix_reviews_created_at     ON crm.reviews (created_at);
CREATE INDEX ix_reviews_customer_id    ON crm.reviews (customer_id);
CREATE INDEX ix_reviews_rental_id      ON crm.reviews (rental_id);
GO