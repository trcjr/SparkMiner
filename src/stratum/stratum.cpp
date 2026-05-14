/*
 * SparkMiner - Stratum Protocol Implementation
 * Stratum v1 client for pool communication
 *
 * Based on BitsyMiner by Justin Williams (GPL v3)
 */

#include <Arduino.h>
#include <ArduinoJson.h>
#include <WiFi.h>
#include <string>
#include <utility>  // For std::swap
#include <board_config.h>
#include "stratum.h"
#include "stratum_line_buffer.h"
#include "stratum_session_diag.h"
#include "../mining/miner.h"
#include "../logging.h"

// ============================================================
// Constants
// ============================================================
#define STRATUM_MSG_BUFFER  512
#define KEEPALIVE_MS        120000
#define INACTIVITY_MS       700000
#define HANDSHAKE_TIMEOUT_MS 12000
#define NO_JOB_WARNING_MS   5000
#define POST_ACCEPT_NOTIFY_WARN_MS 30000
#define MAX_PENDING_REQUESTS 16

// ============================================================
// Global State
// ============================================================
static QueueHandle_t s_submitQueue = NULL;
static submit_entry_t s_pendingResponses[MAX_PENDING_SUBMISSIONS];
static uint16_t s_pendingIndex = 0;

static pool_config_t s_primaryPool;
static pool_config_t s_backupPool;
static bool s_hasBackupPool = false;

static volatile bool s_isConnected = false;
static volatile bool s_socketConnected = false;
static volatile bool s_isSubscribed = false;
static volatile bool s_isAuthorized = false;
static volatile bool s_hasDifficulty = false;
static volatile bool s_hasJob = false;
static volatile bool s_reconnectRequested = false;
static char s_currentPoolUrl[MAX_POOL_URL_LEN] = {0};
static uint32_t s_authorizedAt = 0;
static bool s_noJobWarningLogged = false;

// Stores the fully authorized username (e.g. "wallet.worker") for use in submissions
static char s_authorizedWorkerName[MAX_WALLET_LEN + 34] = {0};

static uint32_t s_messageId = 1;
static uint32_t s_lastActivity = 0;
static uint32_t s_lastSubmit = 0;
static stratum_session_diag_t s_sessionDiag;
static char s_reconnectReason[96] = "none";

// WiFi reconnection state (Issue #4 fix)
static uint32_t s_wifiReconnectAttempts = 0;
static uint32_t s_lastWifiReconnectAttempt = 0;

// Extra nonce from subscription
static char s_extraNonce1[32] = {0};
static int s_extraNonce2Size = 4;

// JSON document for parsing
static StaticJsonDocument<4096> s_doc;
static StratumLineBuffer s_lineBuffer(4096);

typedef enum {
    REQ_NONE = 0,
    REQ_SUBSCRIBE,
    REQ_AUTHORIZE,
    REQ_SUGGEST_DIFFICULTY,
    REQ_KEEPALIVE
} pending_request_type_t;

typedef struct {
    uint32_t id;
    pending_request_type_t type;
    uint32_t sentAt;
} pending_request_t;

static pending_request_t s_pendingRequests[MAX_PENDING_REQUESTS];

// ============================================================
// Utility Functions
// ============================================================

static uint32_t getNextId() {
    if (s_messageId == UINT32_MAX) {
        s_messageId = 1;
    }
    return s_messageId++;
}

// Safe string copy with null termination
static void safeStrCpy(char *dest, const char *src, size_t maxLen) {
    strncpy(dest, src, maxLen - 1);
    dest[maxLen - 1] = '\0';
}

// Format hex string with zero padding (big-endian - value as hex)
static void formatHex8(char *dest, uint32_t value) {
    static const char *hex = "0123456789abcdef";
    for (int i = 7; i >= 0; i--) {
        dest[i] = hex[value & 0xF];
        value >>= 4;
    }
    dest[8] = '\0';
}

static void clearPendingRequests() {
    memset(s_pendingRequests, 0, sizeof(s_pendingRequests));
}

static void requestReconnect(const char *reason) {
    safeStrCpy(s_reconnectReason, reason ? reason : "unspecified", sizeof(s_reconnectReason));
    s_reconnectRequested = true;
    stratum_session_diag_on_reconnect_request(&s_sessionDiag, millis());
}

static const char* requestTypeToString(pending_request_type_t type) {
    switch (type) {
        case REQ_SUBSCRIBE: return "subscribe";
        case REQ_AUTHORIZE: return "authorize";
        case REQ_SUGGEST_DIFFICULTY: return "suggest_difficulty";
        case REQ_KEEPALIVE: return "keepalive";
        default: return "none";
    }
}

static bool addPendingRequest(uint32_t id, pending_request_type_t type) {
    for (int i = 0; i < MAX_PENDING_REQUESTS; i++) {
        if (s_pendingRequests[i].id == 0) {
            s_pendingRequests[i].id = id;
            s_pendingRequests[i].type = type;
            s_pendingRequests[i].sentAt = millis();
            return true;
        }
    }
    Serial.printf("[STRATUM] WARNING: pending request map full, dropping id=%lu type=%s\n",
                  id, requestTypeToString(type));
    return false;
}

static pending_request_type_t takePendingRequest(uint32_t id) {
    for (int i = 0; i < MAX_PENDING_REQUESTS; i++) {
        if (s_pendingRequests[i].id == id) {
            pending_request_type_t type = s_pendingRequests[i].type;
            s_pendingRequests[i].id = 0;
            s_pendingRequests[i].type = REQ_NONE;
            s_pendingRequests[i].sentAt = 0;
            return type;
        }
    }
    return REQ_NONE;
}

static int countPendingRequests() {
    int count = 0;
    for (int i = 0; i < MAX_PENDING_REQUESTS; i++) {
        if (s_pendingRequests[i].id != 0) {
            count++;
        }
    }
    return count;
}

static int countPendingSubmissions() {
    int count = 0;
    for (int i = 0; i < MAX_PENDING_SUBMISSIONS; i++) {
        if (s_pendingResponses[i].msgId != 0) {
            count++;
        }
    }
    return count;
}

static const char* sessionPhaseToString() {
    if (s_isConnected) return "ready";
    if (s_isAuthorized && s_hasJob) return "authorized-waiting-difficulty";
    if (s_isAuthorized) return "authorized-waiting-job";
    if (s_isSubscribed) return "subscribed";
    if (s_socketConnected) return "socket";
    return "disconnected";
}

static void logSessionSnapshot(const char *action, const char *reason, bool clientConnected) {
    uint32_t now = millis();
    log_linef(
        "[STRATUM] %s reason=%s phase=%s client=%d ready=%d flags{sock=%d sub=%d auth=%d diff=%d job=%d} reconnect=%d waiting_post_accept=%d clean_jobs_seen=%d last_clean_jobs=%d current_job=%s submit_job=%s",
        action,
        reason ? reason : "unknown",
        sessionPhaseToString(),
        clientConnected ? 1 : 0,
        s_isConnected ? 1 : 0,
        s_socketConnected ? 1 : 0,
        s_isSubscribed ? 1 : 0,
        s_isAuthorized ? 1 : 0,
        s_hasDifficulty ? 1 : 0,
        s_hasJob ? 1 : 0,
        s_reconnectRequested ? 1 : 0,
        s_sessionDiag.waitingForWorkAfterAcceptedBlock ? 1 : 0,
        s_sessionDiag.hasSeenCleanJobs ? 1 : 0,
        s_sessionDiag.lastNotifyCleanJobs ? 1 : 0,
        s_sessionDiag.currentJobId[0] ? s_sessionDiag.currentJobId : "-",
        s_sessionDiag.lastSubmitJobId[0] ? s_sessionDiag.lastSubmitJobId : "-"
    );
    log_linef(
        "[STRATUM] %s timers_ms now=%lu connected=%lu auth_age=%lu pool=%lu read=%lu write=%lu notify=%lu submit=%lu accepted=%lu accepted_block=%lu waiting=%lu reconnect_req=%lu pending{req=%d submit=%d} prevhash=%s extranonce1=%s en2=%d",
        action,
        (unsigned long)now,
        (unsigned long)s_sessionDiag.connectedAt,
        (unsigned long)(s_authorizedAt ? now - s_authorizedAt : 0),
        (unsigned long)stratum_session_diag_age(now, s_sessionDiag.lastPoolMessageAt),
        (unsigned long)stratum_session_diag_age(now, s_sessionDiag.lastSocketReadAt),
        (unsigned long)stratum_session_diag_age(now, s_sessionDiag.lastSocketWriteAt),
        (unsigned long)stratum_session_diag_age(now, s_sessionDiag.lastNotifyAt),
        (unsigned long)stratum_session_diag_age(now, s_sessionDiag.lastSubmitAt),
        (unsigned long)stratum_session_diag_age(now, s_sessionDiag.lastAcceptedAt),
        (unsigned long)stratum_session_diag_age(now, s_sessionDiag.lastAcceptedBlockAt),
        (unsigned long)stratum_session_diag_age(now, s_sessionDiag.waitingForWorkSinceAt),
        (unsigned long)stratum_session_diag_age(now, s_sessionDiag.lastReconnectRequestAt),
        countPendingRequests(),
        countPendingSubmissions(),
        s_sessionDiag.currentPrevHash[0] ? s_sessionDiag.currentPrevHash : "-",
        s_sessionDiag.extraNonce1[0] ? s_sessionDiag.extraNonce1 : "-",
        s_sessionDiag.extraNonce2Size
    );
}

static void logPendingState() {
    char pendingReqs[192] = {0};
    size_t used = 0;

    for (int i = 0; i < MAX_PENDING_REQUESTS; i++) {
        if (s_pendingRequests[i].id == 0) {
            continue;
        }
        int written = snprintf(
            pendingReqs + used,
            sizeof(pendingReqs) - used,
            "%s%lu:%s",
            used ? "," : "",
            s_pendingRequests[i].id,
            requestTypeToString(s_pendingRequests[i].type)
        );
        if (written <= 0 || (size_t)written >= (sizeof(pendingReqs) - used)) {
            break;
        }
        used += (size_t)written;
    }

    dbg("[STRATUM] Pending map: requests=%d [%s] submits=%d\n",
        countPendingRequests(),
        used ? pendingReqs : "none",
        countPendingSubmissions());
}

static void evaluateReadyState() {
    bool ready = s_socketConnected && s_isSubscribed && s_isAuthorized && s_hasDifficulty && s_hasJob;
    if (ready != s_isConnected) {
        s_isConnected = ready;
        Serial.printf("[STRATUM] State transition: ready=%d (socket=%d subscribed=%d authorized=%d diff=%d job=%d)\n",
                      ready ? 1 : 0,
                      s_socketConnected ? 1 : 0,
                      s_isSubscribed ? 1 : 0,
                      s_isAuthorized ? 1 : 0,
                      s_hasDifficulty ? 1 : 0,
                      s_hasJob ? 1 : 0);
    }
}

static void resetSessionState() {
    s_socketConnected = false;
    s_isSubscribed = false;
    s_isAuthorized = false;
    s_hasDifficulty = true;  // Safe default difficulty until pool overrides.
    s_hasJob = false;
    s_authorizedAt = 0;
    s_noJobWarningLogged = false;
    s_lineBuffer.reset();
    clearPendingRequests();
    memset(s_pendingResponses, 0, sizeof(s_pendingResponses));
    s_pendingIndex = 0;
    s_isConnected = false;
    s_lastActivity = 0;
    s_lastSubmit = 0;
    stratum_session_diag_reset(&s_sessionDiag);
    miner_set_difficulty(1.0);
}

static void maybeLogNoJobWarning() {
    if (s_socketConnected && s_isAuthorized && !s_hasJob && !s_noJobWarningLogged) {
        if (millis() - s_authorizedAt >= NO_JOB_WARNING_MS) {
            Serial.println("[STRATUM] WARNING: authorized but no mining.notify received yet");
            s_noJobWarningLogged = true;
        }
    }
}

// ============================================================
// Protocol Functions
// ============================================================

static bool sendMessage(WiFiClient &client, const char *msg) {
    if (!client.connected()) return false;

    // Send message with newline as single write (like NerdMiner)
    // This avoids TCP packet fragmentation issues
    String fullMsg = String(msg) + "\n";
    client.print(fullMsg);
    uint32_t now = millis();
    s_lastSubmit = now;
    stratum_session_diag_on_socket_write(&s_sessionDiag, now);

    dbg("[STRATUM] TX: %s\n", msg);
    return true;
}

static bool parseSubscribeResponse() {
    if (s_doc.containsKey("error") && !s_doc["error"].isNull()) {
        const char *errMsg = s_doc["error"][1];
        Serial.printf("[STRATUM] Subscribe error: %s\n", errMsg ? errMsg : "unknown");
        return false;
    }

    if (!s_doc.containsKey("result") || !s_doc["result"].is<JsonArray>()) {
        Serial.println("[STRATUM] Invalid subscribe response (no result)");
        return false;
    }

    // Extract extra nonce
    const char *en1 = s_doc["result"][1];
    if (en1) {
        safeStrCpy(s_extraNonce1, en1, sizeof(s_extraNonce1));
    }

    s_extraNonce2Size = s_doc["result"][2] | 4;

    // Pass to miner
    miner_set_extranonce(s_extraNonce1, s_extraNonce2Size);

    dbg("[STRATUM] Subscribed: extraNonce1=%s, extraNonce2Size=%d\n",
        s_extraNonce1, s_extraNonce2Size);

    s_isSubscribed = true;
    Serial.println("[STRATUM] State transition: subscribed=1");

    return true;
}

static bool parseAuthorizeResponse() {
    if (s_doc.containsKey("error") && !s_doc["error"].isNull()) {
        const char *errMsg = s_doc["error"][1];
        Serial.printf("[STRATUM] Auth error: %s\n", errMsg ? errMsg : "unknown");
        return false;
    }

    bool result = s_doc["result"] | false;
    s_isAuthorized = result;
    s_authorizedAt = result ? millis() : 0;
    Serial.printf("[STRATUM] State transition: authorized=%d\n", result ? 1 : 0);
    return result;
}

static void parseMiningNotify() {
    if (!s_doc.containsKey("params")) return;

    JsonArray params = s_doc["params"];

    // Use static job to avoid stack allocation of large struct each time
    static stratum_job_t job;
    memset(&job, 0, sizeof(job));

    // Copy strings to fixed char arrays (no heap allocation!)
    const char *p0 = params[0];
    const char *p1 = params[1];
    const char *p2 = params[2];
    const char *p3 = params[3];
    const char *p5 = params[5];
    const char *p6 = params[6];
    const char *p7 = params[7];

    if (p0) strncpy(job.jobId, p0, STRATUM_JOB_ID_LEN - 1);
    if (p1) strncpy(job.prevHash, p1, STRATUM_PREVHASH_LEN - 1);
    if (p2) {
        strncpy(job.coinBase1, p2, STRATUM_COINBASE1_LEN - 1);
        if (strlen(p2) >= STRATUM_COINBASE1_LEN)
            Serial.printf("[STRATUM] WARNING: coinBase1 truncated (%d chars, max %d)\n", strlen(p2), STRATUM_COINBASE1_LEN - 1);
    }
    if (p3) {
        strncpy(job.coinBase2, p3, STRATUM_COINBASE2_LEN - 1);
        if (strlen(p3) >= STRATUM_COINBASE2_LEN)
            Serial.printf("[STRATUM] WARNING: coinBase2 truncated (%d chars, max %d)\n", strlen(p3), STRATUM_COINBASE2_LEN - 1);
    }
    if (p5) strncpy(job.version, p5, STRATUM_FIELD_LEN - 1);
    if (p6) strncpy(job.nbits, p6, STRATUM_FIELD_LEN - 1);
    if (p7) strncpy(job.ntime, p7, STRATUM_FIELD_LEN - 1);

    // Copy merkle branches to fixed array
    JsonArray merkle = params[4];
    job.merkleBranchCount = 0;
    for (size_t i = 0; i < merkle.size() && i < STRATUM_MAX_MERKLE; i++) {
        const char *branch = merkle[i];
        if (branch) {
            strncpy(job.merkleBranches[i], branch, 67);
            job.merkleBranches[i][67] = '\0';
            job.merkleBranchCount++;
        }
    }

    job.cleanJobs = params[8] | false;
    strncpy(job.extraNonce1, s_extraNonce1, STRATUM_EXTRANONCE_LEN - 1);
    job.extraNonce2Size = s_extraNonce2Size;

    bool prevHashChanged = s_sessionDiag.currentPrevHash[0] && strcmp(s_sessionDiag.currentPrevHash, job.prevHash) != 0;
    bool jobChanged = s_sessionDiag.currentJobId[0] && strcmp(s_sessionDiag.currentJobId, job.jobId) != 0;
    s_lastActivity = millis();
    s_hasJob = true;
    stratum_session_diag_on_notify(&s_sessionDiag, s_lastActivity, job.jobId, job.prevHash, job.cleanJobs);
    Serial.printf(
        "[STRATUM] Notify job=%s clean_jobs=%d prevhash_changed=%d job_changed=%d merkle=%d\n",
        job.jobId,
        job.cleanJobs ? 1 : 0,
        prevHashChanged ? 1 : 0,
        jobChanged ? 1 : 0,
        job.merkleBranchCount
    );
    Serial.println("[STRATUM] State transition: job_ready=1 (mining.notify received)");
    miner_start_job(&job);
}

static void parseSetDifficulty() {
    if (!s_doc.containsKey("params")) return;

    double diff = s_doc["params"][0] | 1.0;

    if (!isnan(diff) && diff > 0) {
        miner_set_difficulty(diff);
        s_hasDifficulty = true;
        Serial.printf("[STRATUM] State transition: difficulty_ready=1 (%.6f)\n", diff);
        dbg("[STRATUM] Pool difficulty: %.4f\n", diff);
    }
}

static void parseSetExtranonce() {
    if (!s_doc.containsKey("params") || !s_doc["params"].is<JsonArray>()) {
        return;
    }

    JsonArray params = s_doc["params"];
    const char *en1 = params[0] | "";
    int en2Size = params[1] | s_extraNonce2Size;
    char previousEn1[sizeof(s_extraNonce1)];
    safeStrCpy(previousEn1, s_extraNonce1, sizeof(previousEn1));
    int previousEn2Size = s_extraNonce2Size;

    if (en1[0]) {
        safeStrCpy(s_extraNonce1, en1, sizeof(s_extraNonce1));
    }
    if (en2Size > 0) {
        s_extraNonce2Size = en2Size;
    }

    miner_set_extranonce(s_extraNonce1, s_extraNonce2Size);
    stratum_session_diag_on_extranonce(&s_sessionDiag, s_extraNonce1, s_extraNonce2Size);
    Serial.printf(
        "[STRATUM] Updated extranonce: en1=%s en2_size=%d changed=%d\n",
        s_extraNonce1,
        s_extraNonce2Size,
        (strcmp(previousEn1, s_extraNonce1) != 0 || previousEn2Size != s_extraNonce2Size) ? 1 : 0
    );
}

static bool handleSubmissionResponse(uint32_t msgId, bool accepted) {
    for (int i = 0; i < MAX_PENDING_SUBMISSIONS; i++) {
        if (s_pendingResponses[i].msgId == msgId) {
            mining_stats_t *stats = miner_get_stats();
            bool wasBlockCandidate = (s_pendingResponses[i].flags & SUBMIT_FLAG_BLOCK) != 0;

            uint32_t latency = millis() - s_pendingResponses[i].sentTime;
            stats->lastLatency = latency;
            stats->avgLatency = (stats->avgLatency == 0) ? latency : ((stats->avgLatency * 9 + latency) / 10);

            if (accepted) {
                stats->accepted++;
                dbg("[STRATUM] Share accepted!\n");
                Serial.printf(
                    "[STRATUM] Share accepted: job=%s msg=%lu block_candidate=%d latency=%lu\n",
                    s_pendingResponses[i].jobId,
                    (unsigned long)msgId,
                    wasBlockCandidate ? 1 : 0,
                    (unsigned long)latency
                );
            } else {
                stats->rejected++;
                const char *reason = s_doc["error"][1] | "unknown";
                dbg("[STRATUM] Share rejected: %s\n", reason);
                Serial.printf("[STRATUM] Share rejected: %s\n", reason);
            }

            if (s_pendingResponses[i].callback) {
                const char *reason = accepted ? NULL : (const char *)s_doc["error"][1];
                s_pendingResponses[i].callback(
                    s_pendingResponses[i].sessionId,
                    s_pendingResponses[i].msgId,
                    accepted,
                    reason
                );
            }

            stratum_session_diag_on_submit_result(&s_sessionDiag, millis(), accepted, wasBlockCandidate);

            s_pendingResponses[i].msgId = 0;
            return true;
        }
    }
    return false;
}

static void handleResponseById(uint32_t msgId) {
    bool accepted = s_doc["result"] | false;

    if (handleSubmissionResponse(msgId, accepted)) {
        return;
    }

    pending_request_type_t reqType = takePendingRequest(msgId);
    switch (reqType) {
        case REQ_SUBSCRIBE:
            parseSubscribeResponse();
            break;
        case REQ_AUTHORIZE:
            if (!parseAuthorizeResponse()) {
                Serial.println("[STRATUM] Authorization failed");
            }
            break;
        case REQ_SUGGEST_DIFFICULTY:
        case REQ_KEEPALIVE:
            break;
        case REQ_NONE:
        default:
            dbg("[STRATUM] Unmatched response id=%lu\n", msgId);
            break;
    }
}

static void handleNotification(const char *method) {
    if (strcmp(method, "mining.notify") == 0) {
        parseMiningNotify();
    } else if (strcmp(method, "mining.set_difficulty") == 0) {
        parseSetDifficulty();
    } else if (strcmp(method, "mining.set_extranonce") == 0) {
        parseSetExtranonce();
    } else if (strcmp(method, "client.reconnect") == 0) {
        Serial.println("[STRATUM] Server requested client.reconnect");
        requestReconnect("server_client.reconnect");
    } else if (strcmp(method, "client.show_message") == 0) {
        const char *msg = s_doc["params"][0] | "";
        Serial.printf("[STRATUM] Server message: %s\n", msg);
    } else {
        dbg("[STRATUM] Unknown method: %s\n", method);
    }
}

static void handleServerLine(const String &line) {
    if (line.length() == 0) {
        return;
    }

    dbg("[STRATUM] RX: %s\n", line.c_str());

    s_doc.clear();
    DeserializationError err = deserializeJson(s_doc, line);
    if (err) {
        dbg("[STRATUM] Parse error: %s\n", err.c_str());
        return;
    }

    s_lastActivity = millis();
    stratum_session_diag_on_pool_message(&s_sessionDiag, s_lastActivity);

    if (s_doc.containsKey("method")) {
        const char *method = s_doc["method"] | "";
        Serial.printf("[STRATUM] Incoming notification method=%s\n", method);
        handleNotification(method);
        logPendingState();
        evaluateReadyState();
        return;
    }

    if (s_doc.containsKey("id")) {
        uint32_t msgId = s_doc["id"] | 0;
        Serial.printf("[STRATUM] Incoming response id=%lu\n", msgId);
        handleResponseById(msgId);
        logPendingState();
        evaluateReadyState();
    }
}

static void drainClientMessages(WiFiClient &client) {
    std::string outLine;
    bool droppedLongLine = false;

    while (client.available() > 0) {
        char c = (char)client.read();
        stratum_session_diag_on_socket_read(&s_sessionDiag, millis());
        if (s_lineBuffer.push(c, outLine, droppedLongLine)) {
            String line(outLine.c_str());
            line.trim();
            handleServerLine(line);
        }
        if (droppedLongLine) {
            Serial.println("[STRATUM] WARNING: Line exceeded max length, discarded");
        }
    }
}

static bool startSession(WiFiClient &client, const char *wallet, const char *password, const char *workerName) {
    char msg[STRATUM_MSG_BUFFER];
    resetSessionState();
    s_socketConnected = true;
    stratum_session_diag_on_socket_connected(&s_sessionDiag, millis());

    // Mining.authorize - append worker name if set
    char fullUsername[MAX_WALLET_LEN + 34];
    if (workerName && workerName[0]) {
        snprintf(fullUsername, sizeof(fullUsername), "%s.%s", wallet, workerName);
    } else {
        safeStrCpy(fullUsername, wallet, sizeof(fullUsername));
    }

    // Store authorized worker name for submissions
    safeStrCpy(s_authorizedWorkerName, fullUsername, sizeof(s_authorizedWorkerName));

    // Mining.subscribe
    uint32_t subId = getNextId();
    snprintf(msg, sizeof(msg),
        "{\"id\":%lu,\"method\":\"mining.subscribe\",\"params\":[\"%s/%s\"]}",
        subId, MINER_NAME, AUTO_VERSION);
    if (!sendMessage(client, msg)) return false;
    addPendingRequest(subId, REQ_SUBSCRIBE);

    // Suggest difficulty
    uint32_t diffId = getNextId();
    snprintf(msg, sizeof(msg),
        "{\"id\":%lu,\"method\":\"mining.suggest_difficulty\",\"params\":[%.10g]}",
        diffId, DESIRED_DIFFICULTY);
    sendMessage(client, msg);
    addPendingRequest(diffId, REQ_SUGGEST_DIFFICULTY);

    uint32_t authId = getNextId();
    snprintf(msg, sizeof(msg),
        "{\"id\":%lu,\"method\":\"mining.authorize\",\"params\":[\"%s\",\"%s\"]}",
        authId, fullUsername, password);
    if (!sendMessage(client, msg)) return false;
    addPendingRequest(authId, REQ_AUTHORIZE);

    const uint32_t deadline = millis() + HANDSHAKE_TIMEOUT_MS;
    while (client.connected() && (int32_t)(deadline - millis()) > 0) {
        drainClientMessages(client);
        maybeLogNoJobWarning();

        if (s_isSubscribed && s_isAuthorized) {
            Serial.printf("[STRATUM] Authorized as %s\n", fullUsername);
            return true;
        }

        vTaskDelay(10 / portTICK_PERIOD_MS);
    }

    Serial.printf("[STRATUM] Handshake timeout (subscribed=%d authorized=%d)\n",
                  s_isSubscribed ? 1 : 0,
                  s_isAuthorized ? 1 : 0);

    return false;
}

static void submitShare(WiFiClient &client, const submit_entry_t *entry) {
    char msg[STRATUM_MSG_BUFFER];
    char timestamp[9], nonce[9];

    // Format as 8-char hex (value as hex, zero-padded)
    formatHex8(timestamp, entry->timestamp);
    formatHex8(nonce, entry->nonce);

    uint32_t msgId = getNextId();

    // Standard Stratum v1 submit (5 params, no version rolling)
    snprintf(msg, sizeof(msg),
        "{\"id\":%lu,\"method\":\"mining.submit\",\"params\":[\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"]}",
        msgId,
        s_authorizedWorkerName, // Use the full worker name used during authorization
        entry->jobId,
        entry->extraNonce2,
        timestamp,
        nonce);

    Serial.printf("[STRATUM] Submit: job=%s en2=%s time=%s nonce=%s\n",
        entry->jobId, entry->extraNonce2, timestamp, nonce);

    if (sendMessage(client, msg)) {
        // Store in pending responses for latency tracking
        submit_entry_t pending = *entry;
        pending.msgId = msgId;
        pending.sentTime = millis();

        s_pendingResponses[s_pendingIndex] = pending;
        s_pendingIndex = (s_pendingIndex + 1) % MAX_PENDING_SUBMISSIONS;

        s_lastSubmit = pending.sentTime;
        stratum_session_diag_on_submit(&s_sessionDiag, pending.sentTime, entry->jobId, (entry->flags & SUBMIT_FLAG_BLOCK) != 0);
        miner_get_stats()->shares++;
    }
}

// ============================================================
// Public API
// ============================================================

void stratum_init() {
    // Create submission queue
    s_submitQueue = xQueueCreate(MAX_PENDING_SUBMISSIONS, sizeof(submit_entry_t));

    // Initialize pending responses
    memset(s_pendingResponses, 0, sizeof(s_pendingResponses));
    stratum_session_diag_reset(&s_sessionDiag);

    // Set default pool
    safeStrCpy(s_primaryPool.url, DEFAULT_POOL_URL, MAX_POOL_URL_LEN);
    s_primaryPool.port = DEFAULT_POOL_PORT;
    safeStrCpy(s_primaryPool.password, DEFAULT_POOL_PASS, MAX_PASSWORD_LEN);

    dbg("[STRATUM] Initialized\n");
}

void stratum_task(void *param) {
    WiFiClient client;
    bool usingBackup = false;
    uint32_t lastConnectAttempt = 0;
    uint32_t backupConnectTime = 0;

    log_wait_startup_barrier();
    log_linef("[STRATUM] Task started on core %d", xPortGetCoreID());

    while (true) {
        // Wait for WiFi with auto-reconnect (Issue #4 fix)
        if (WiFi.status() != WL_CONNECTED) {
            if (s_socketConnected || s_isConnected) {
                logSessionSnapshot("disconnect", "wifi_lost", client.connected());
                miner_stop();
                client.stop();
                resetSessionState();
                log_line("[WIFI] Connection lost, attempting reconnect...");
            }

            // Calculate exponential backoff: 1s, 2s, 4s, 8s, 15s max
            uint32_t backoffMs = 1000 * (1 << min(s_wifiReconnectAttempts, (uint32_t)4));
            if (backoffMs > 15000) backoffMs = 15000;

            if (millis() - s_lastWifiReconnectAttempt >= backoffMs) {
                s_wifiReconnectAttempts++;
                s_lastWifiReconnectAttempt = millis();
                log_linef("[WIFI] Reconnect attempt %lu (backoff: %lums)",
                          s_wifiReconnectAttempts, backoffMs);
                WiFi.reconnect();
            }

            vTaskDelay(500 / portTICK_PERIOD_MS);
            continue;
        }

        // WiFi connected - reset reconnect counter
        if (s_wifiReconnectAttempts > 0) {
            log_linef("[WIFI] Reconnected after %lu attempts", s_wifiReconnectAttempts);
            s_wifiReconnectAttempts = 0;
        }

        // Check pool configuration
        if (!s_primaryPool.url[0] || !s_primaryPool.port) {
            dbg("[STRATUM] No pool configured\n");
            vTaskDelay(5000 / portTICK_PERIOD_MS);
            continue;
        }

        if (!s_primaryPool.wallet[0]) {
            dbg("[STRATUM] No wallet configured\n");
            vTaskDelay(5000 / portTICK_PERIOD_MS);
            continue;
        }

        // Handle reconnect request
        if (s_reconnectRequested) {
            logSessionSnapshot("disconnect", s_reconnectReason, client.connected());
            miner_stop();
            client.stop();
            resetSessionState();
            s_reconnectRequested = false;
            safeStrCpy(s_reconnectReason, "none", sizeof(s_reconnectReason));
            vTaskDelay(100 / portTICK_PERIOD_MS);
            continue;
        }

        // Connect if needed
        if (!client.connected()) {
            if (s_socketConnected || s_isConnected) {
                logSessionSnapshot("disconnect", "socket_closed_remote_or_transport", false);
                miner_stop();
                resetSessionState();
            }

            usingBackup = false;

            logSessionSnapshot("reconnect", "connect_primary", false);
            log_linef("[STRATUM] Connecting to %s:%d...",
                      s_primaryPool.url, s_primaryPool.port);

            // STABILITY FIX: Use connect timeout (10s) to prevent long blocks
            if (client.connect(s_primaryPool.url, s_primaryPool.port, 10000)) {
                if (startSession(client, s_primaryPool.wallet, s_primaryPool.password, s_primaryPool.workerName)) {
                    s_lastActivity = millis();
                    safeStrCpy(s_currentPoolUrl, s_primaryPool.url, MAX_POOL_URL_LEN);
                    logSessionSnapshot("reconnect", "connected_primary", client.connected());
                    log_line("[STRATUM] Connected to primary pool");
                } else {
                    logSessionSnapshot("disconnect", "handshake_failed_primary", client.connected());
                    client.stop();
                    resetSessionState();
                }
            } else {
                log_line("[STRATUM] Connection failed");

                // Try backup pool after 30s of failures
                if (s_hasBackupPool && (millis() - lastConnectAttempt > POOL_FAILOVER_MS)) {
                    logSessionSnapshot("reconnect", "connect_backup", false);
                    log_linef("[STRATUM] Trying backup: %s:%d",
                              s_backupPool.url, s_backupPool.port);

                    // STABILITY FIX: Use connect timeout (10s)
                    if (client.connect(s_backupPool.url, s_backupPool.port, 10000)) {
                        if (startSession(client, s_backupPool.wallet, s_backupPool.password, s_backupPool.workerName)) {
                            usingBackup = true;
                            backupConnectTime = millis();
                            s_lastActivity = millis();
                            safeStrCpy(s_currentPoolUrl, s_backupPool.url, MAX_POOL_URL_LEN);
                            logSessionSnapshot("reconnect", "connected_backup", client.connected());
                            log_line("[STRATUM] Connected to backup pool");
                        } else {
                            logSessionSnapshot("disconnect", "handshake_failed_backup", client.connected());
                            client.stop();
                            resetSessionState();
                        }
                    }
                }
            }

            lastConnectAttempt = millis();

            if (!s_socketConnected) {
                vTaskDelay(10000 / portTICK_PERIOD_MS);
                continue;
            }
        }

        // Try to switch back from backup after 2 minutes
        if (usingBackup && (millis() - backupConnectTime > 120000)) {
            // STABILITY FIX: Use connect timeout and avoid shallow copy of WiFiClient
            // Test connection to primary pool first
            WiFiClient testClient;
            if (testClient.connect(s_primaryPool.url, s_primaryPool.port, 10000)) {
                if (startSession(testClient, s_primaryPool.wallet, s_primaryPool.password, s_primaryPool.workerName)) {
                    // Successfully connected to primary - switch over
                    logSessionSnapshot("disconnect", "switch_to_primary", client.connected());
                    miner_stop();
                    client.stop();
                    // Use swap to safely transfer the connection instead of shallow copy
                    std::swap(client, testClient);
                    testClient.stop();  // Clean up the old (now empty) client
                    usingBackup = false;
                    safeStrCpy(s_currentPoolUrl, s_primaryPool.url, MAX_POOL_URL_LEN);
                    logSessionSnapshot("reconnect", "switched_to_primary", client.connected());
                    log_line("[STRATUM] Switched back to primary pool");
                    continue;
                } else {
                    testClient.stop();
                }
            }
            backupConnectTime = millis();  // Try again later
        }

        // Handle incoming messages
        drainClientMessages(client);
        maybeLogNoJobWarning();

        if (stratum_session_diag_should_warn_waiting_for_work(&s_sessionDiag, millis(), POST_ACCEPT_NOTIFY_WARN_MS)) {
            logSessionSnapshot("watchdog", "accepted_block_waiting_for_notify", client.connected());
            log_line("[STRATUM] Waiting for fresh job after accepted block; keeping connection open");
        }

        // Process submission queue
        submit_entry_t entry;
        while (xQueueReceive(s_submitQueue, &entry, 0) == pdTRUE) {
            submitShare(client, &entry);
        }

        // Send keepalive if idle
        if (s_sessionDiag.lastSocketWriteAt > 0 && millis() - s_sessionDiag.lastSocketWriteAt > KEEPALIVE_MS) {
            char msg[STRATUM_MSG_BUFFER];
            uint32_t keepId = getNextId();
            snprintf(msg, sizeof(msg),
                "{\"id\":%lu,\"method\":\"mining.suggest_difficulty\",\"params\":[%.10g]}",
                keepId, DESIRED_DIFFICULTY);
            sendMessage(client, msg);
            addPendingRequest(keepId, REQ_KEEPALIVE);
        }

        // Check for inactivity
        if (stratum_session_diag_should_disconnect_for_inactivity(&s_sessionDiag, millis(), INACTIVITY_MS)) {
            logSessionSnapshot("disconnect", "pool_inactive", client.connected());
            log_line("[STRATUM] Pool inactive, disconnecting");
            miner_stop();
            client.stop();
            resetSessionState();
        }

        vTaskDelay(100 / portTICK_PERIOD_MS);
    }
}

bool stratum_submit_share(const submit_entry_t *entry) {
    if (!s_submitQueue) return false;
    if (xQueueSend(s_submitQueue, entry, pdMS_TO_TICKS(100)) == pdTRUE) {
        return true;
    }
    logSessionSnapshot("queue", "submit_queue_full", s_socketConnected);
    return false;
}

void stratum_reconnect() {
    requestReconnect("external_request");
}

bool stratum_is_connected() {
    return s_isConnected;
}

bool stratum_is_backup() {
    if (!s_isConnected) return false;
    // Compare current URL with primary URL
    return strncmp(s_currentPoolUrl, s_primaryPool.url, MAX_POOL_URL_LEN) != 0;
}

uint8_t stratum_get_state_flags() {
    uint8_t flags = 0;
    if (s_socketConnected) flags |= STRATUM_STATE_SOCKET_CONNECTED;
    if (s_isSubscribed) flags |= STRATUM_STATE_SUBSCRIBED;
    if (s_isAuthorized) flags |= STRATUM_STATE_AUTHORIZED;
    if (s_hasDifficulty) flags |= STRATUM_STATE_DIFFICULTY_READY;
    if (s_hasJob) flags |= STRATUM_STATE_JOB_READY;
    return flags;
}

const char* stratum_get_pool() {
    return s_currentPoolUrl;
}

void stratum_set_pool(const char *url, int port, const char *wallet, const char *password, const char *workerName) {
    safeStrCpy(s_primaryPool.url, url, MAX_POOL_URL_LEN);
    s_primaryPool.port = port;
    safeStrCpy(s_primaryPool.wallet, wallet, MAX_WALLET_LEN);
    safeStrCpy(s_primaryPool.password, password, MAX_PASSWORD_LEN);
    if (workerName) {
        safeStrCpy(s_primaryPool.workerName, workerName, 32);
    } else {
        s_primaryPool.workerName[0] = '\0';
    }
}

void stratum_set_backup_pool(const char *url, int port, const char *wallet, const char *password, const char *workerName) {
    safeStrCpy(s_backupPool.url, url, MAX_POOL_URL_LEN);
    s_backupPool.port = port;
    safeStrCpy(s_backupPool.wallet, wallet, MAX_WALLET_LEN);
    safeStrCpy(s_backupPool.password, password, MAX_PASSWORD_LEN);
    if (workerName) {
        safeStrCpy(s_backupPool.workerName, workerName, 32);
    } else {
        s_backupPool.workerName[0] = '\0';
    }
    s_hasBackupPool = (url[0] && port > 0 && wallet[0]);
}