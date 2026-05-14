/*
 * SparkMiner - Stratum Protocol
 * Stratum v1 client for pool communication
 *
 * Based on BitsyMiner by Justin Williams (GPL v3)
 *
 * Features:
 * - FreeRTOS queue for async submissions
 * - Callback mechanism for response tracking
 * - Primary/backup pool failover
 */

#ifndef STRATUM_H
#define STRATUM_H

#include <Arduino.h>
#include <WiFiClient.h>
#include "stratum_types.h"

/**
 * Initialize stratum subsystem
 * Creates message queue and initializes state
 */
void stratum_init();

/**
 * Main stratum task (runs on Core 0)
 * Handles pool connection, message parsing, and submissions
 */
void stratum_task(void *param);

/**
 * Submit a share to the pool
 * Thread-safe - can be called from any task
 *
 * @param entry Share data to submit
 * @return true if queued successfully
 */
bool stratum_submit_share(const submit_entry_t *entry);

/**
 * Force reconnect to pool
 * Used after settings change
 */
void stratum_reconnect();

/**
 * Check if connected to pool
 */
bool stratum_is_connected();

// Bit flags returned by stratum_get_state_flags()
#define STRATUM_STATE_SOCKET_CONNECTED (1u << 0)
#define STRATUM_STATE_SUBSCRIBED       (1u << 1)
#define STRATUM_STATE_AUTHORIZED       (1u << 2)
#define STRATUM_STATE_DIFFICULTY_READY (1u << 3)
#define STRATUM_STATE_JOB_READY        (1u << 4)

/**
 * Check if currently connected to backup pool
 */
bool stratum_is_backup();

/**
 * Get startup/readiness state bits for compact monitor logging.
 */
uint8_t stratum_get_state_flags();

/**
 * Get current pool URL
 */
const char* stratum_get_pool();

/**
 * Set pool configuration
 * @param workerName Optional worker name (appended as wallet.worker)
 */
void stratum_set_pool(const char *url, int port, const char *wallet, const char *password, const char *workerName = NULL);

/**
 * Set backup pool configuration
 */
void stratum_set_backup_pool(const char *url, int port, const char *wallet, const char *password, const char *workerName = NULL);

#endif // STRATUM_H
