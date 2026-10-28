-- SwiftTrack PostgreSQL Schema Initialization
-- Each service has its own dedicated schema / namespace

CREATE SCHEMA IF NOT EXISTS auth;
CREATE SCHEMA IF NOT EXISTS orders;
CREATE SCHEMA IF NOT EXISTS dispatch;
CREATE SCHEMA IF NOT EXISTS delivery;
CREATE SCHEMA IF NOT EXISTS legacy_cms;
CREATE SCHEMA IF NOT EXISTS legacy_wms;
CREATE SCHEMA IF NOT EXISTS legacy_ros;

-- ============================================================================
-- AUTH SERVICE SCHEMA
-- ============================================================================
CREATE TABLE IF NOT EXISTS auth.users (
    "userID"   SERIAL PRIMARY KEY,
    username   TEXT NOT NULL UNIQUE,
    password   TEXT NOT NULL,
    role       TEXT NOT NULL DEFAULT 'vendor'
);

CREATE TABLE IF NOT EXISTS auth.sessions (
    token      TEXT PRIMARY KEY,
    "userID"   INTEGER NOT NULL REFERENCES auth.users("userID"),
    expires_at TIMESTAMP DEFAULT (NOW() + INTERVAL '24 hours')
);

-- ============================================================================
-- ORDER SERVICE SCHEMA
-- ============================================================================
CREATE TABLE IF NOT EXISTS orders.orders (
    "orderID"         SERIAL PRIMARY KEY,
    "userID"          TEXT,
    "deliveryAddress" TEXT,
    "receiverName"    TEXT,
    "packageName"     TEXT,
    status            TEXT DEFAULT 'PROCESSING',
    "createdAt"       TEXT,
    "cancelledAt"     TEXT,
    "cmsRefID"        INTEGER
);

CREATE TABLE IF NOT EXISTS orders.order_packages (
    id              SERIAL PRIMARY KEY,
    "orderID"       INTEGER NOT NULL REFERENCES orders.orders("orderID"),
    "packageID"     TEXT NOT NULL,
    "packageType"   TEXT,
    "packageWeight" REAL
);

-- ============================================================================
-- DISPATCH SERVICE SCHEMA
-- ============================================================================
CREATE TABLE IF NOT EXISTS dispatch.drivers (
    "driverID" SERIAL PRIMARY KEY,
    name       TEXT,
    status     TEXT DEFAULT 'AVAILABLE'
);

CREATE TABLE IF NOT EXISTS dispatch.manifests (
    "manifestID"   SERIAL PRIMARY KEY,
    "driverID"     INTEGER REFERENCES dispatch.drivers("driverID"),
    "dispatchedAt" TEXT
);

CREATE TABLE IF NOT EXISTS dispatch.manifest_entries (
    "entryID"         SERIAL PRIMARY KEY,
    "manifestID"      INTEGER REFERENCES dispatch.manifests("manifestID"),
    "orderID"         TEXT,
    "deliveryAddress" TEXT
);

-- ============================================================================
-- DELIVERY SERVICE SCHEMA
-- ============================================================================
CREATE TABLE IF NOT EXISTS delivery.manifests (
    "driverID"     INTEGER PRIMARY KEY,
    "manifestID"   INTEGER,
    "driverName"   TEXT,
    status         TEXT DEFAULT 'PENDING',
    "manifestData" JSONB,
    "updatedAt"    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS delivery.orders (
    "orderID"    TEXT PRIMARY KEY,
    "driverID"   INTEGER,
    "manifestID" INTEGER,
    status       TEXT,
    "updatedAt"  TIMESTAMP DEFAULT NOW()
);

-- ============================================================================
-- LEGACY CMS SCHEMA
-- ============================================================================
CREATE TABLE IF NOT EXISTS legacy_cms.orders (
    id                SERIAL PRIMARY KEY,
    "userID"          TEXT NOT NULL,
    "deliveryAddress" TEXT NOT NULL,
    "receiverName"    TEXT NOT NULL,
    "packageType"     TEXT NOT NULL,
    status            TEXT NOT NULL DEFAULT 'ACTIVE',
    "createdAt"       TEXT NOT NULL,
    "cancelledAt"     TEXT
);

CREATE TABLE IF NOT EXISTS legacy_cms.order_packages (
    id          SERIAL PRIMARY KEY,
    "orderID"   INTEGER NOT NULL REFERENCES legacy_cms.orders(id),
    "packageID" TEXT NOT NULL
);

-- ============================================================================
-- LEGACY WMS SCHEMA
-- ============================================================================
CREATE TABLE IF NOT EXISTS legacy_wms.packages (
    id           SERIAL PRIMARY KEY,
    "orderID"    TEXT NOT NULL,
    package_type TEXT,
    package_name TEXT,
    weight       REAL,
    status       TEXT DEFAULT 'Ready'
);

-- ============================================================================
-- LEGACY ROS SCHEMA
-- ============================================================================
CREATE TABLE IF NOT EXISTS legacy_ros.order_address (
    order_id TEXT PRIMARY KEY,
    address  TEXT
);
