-- SwiftTrack Legacy Backends PostgreSQL Schemas
-- Schemas: legacy_cms, legacy_wms, legacy_ros

CREATE SCHEMA IF NOT EXISTS legacy_cms;
CREATE SCHEMA IF NOT EXISTS legacy_wms;
CREATE SCHEMA IF NOT EXISTS legacy_ros;

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
