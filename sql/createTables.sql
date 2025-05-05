-- Table: public.Order

-- DROP TABLE IF EXISTS public."Order";

CREATE TABLE IF NOT EXISTS public."Order"
(
    "Id" integer NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
    "Account" integer NOT NULL,
    "Amount" numeric NOT NULL,
    "Timestamp" time with time zone,
    "RewardAmount" integer,
    CONSTRAINT "Order_pkey" PRIMARY KEY ("Id"),
    CONSTRAINT "Account" FOREIGN KEY ("Account")
        REFERENCES public."Account" ("Id") MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
        NOT VALID
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."Order"
    OWNER to postgres;

	-- Table: public.Account

-- DROP TABLE IF EXISTS public."Account";

CREATE TABLE IF NOT EXISTS public."Account"
(
    "Name" text COLLATE pg_catalog."default",
    "Rewards" numeric,
    "Id" integer NOT NULL GENERATED ALWAYS AS IDENTITY ( CYCLE INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
    CONSTRAINT "Account_pkey" PRIMARY KEY ("Id")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."Account"
    OWNER to postgres;

-- Table: public.RewardsConfig

-- DROP TABLE IF EXISTS public."RewardsConfig";

CREATE TABLE IF NOT EXISTS public."RewardsConfig"
(
    "Id" integer NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
    "RewardPoints" integer NOT NULL,
    "DollarAmount" double precision NOT NULL,
    "TimeStart" text COLLATE pg_catalog."default",
    "TimeEnd" text COLLATE pg_catalog."default",
    CONSTRAINT "RewardsConfig_pkey" PRIMARY KEY ("Id")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."RewardsConfig"
    OWNER to postgres;	