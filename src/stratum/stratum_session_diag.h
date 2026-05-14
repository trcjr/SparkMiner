#ifndef STRATUM_SESSION_DIAG_H
#define STRATUM_SESSION_DIAG_H

#include <stdint.h>
#include <string.h>

#define STRATUM_DIAG_JOB_ID_LEN 32
#define STRATUM_DIAG_PREVHASH_LEN 68
#define STRATUM_DIAG_EXTRANONCE_LEN 32

typedef struct {
    uint32_t connectedAt;
    uint32_t lastSocketReadAt;
    uint32_t lastSocketWriteAt;
    uint32_t lastPoolMessageAt;
    uint32_t lastNotifyAt;
    uint32_t lastSubmitAt;
    uint32_t lastAcceptedAt;
    uint32_t lastAcceptedBlockAt;
    uint32_t lastReconnectRequestAt;
    uint32_t waitingForWorkSinceAt;
    uint32_t lastWaitWarningAt;
    uint32_t lastCleanJobsAt;
    bool waitingForWorkAfterAcceptedBlock;
    bool hasSeenCleanJobs;
    bool lastNotifyCleanJobs;
    bool lastSubmitWasBlock;
    char currentJobId[STRATUM_DIAG_JOB_ID_LEN];
    char currentPrevHash[STRATUM_DIAG_PREVHASH_LEN];
    char lastSubmitJobId[STRATUM_DIAG_JOB_ID_LEN];
    char extraNonce1[STRATUM_DIAG_EXTRANONCE_LEN];
    int extraNonce2Size;
} stratum_session_diag_t;

static inline void stratum_diag_copy(char *dest, const char *src, size_t len) {
    if (len == 0) {
        return;
    }
    if (src == NULL) {
        dest[0] = '\0';
        return;
    }
    strncpy(dest, src, len - 1);
    dest[len - 1] = '\0';
}

static inline void stratum_session_diag_reset(stratum_session_diag_t *diag) {
    if (diag == NULL) {
        return;
    }
    memset(diag, 0, sizeof(*diag));
}

static inline void stratum_session_diag_on_socket_connected(stratum_session_diag_t *diag, uint32_t now) {
    if (diag == NULL) {
        return;
    }
    diag->connectedAt = now;
    diag->lastSocketReadAt = now;
    diag->lastSocketWriteAt = now;
    diag->lastPoolMessageAt = now;
}

static inline void stratum_session_diag_on_socket_read(stratum_session_diag_t *diag, uint32_t now) {
    if (diag == NULL) {
        return;
    }
    diag->lastSocketReadAt = now;
}

static inline void stratum_session_diag_on_socket_write(stratum_session_diag_t *diag, uint32_t now) {
    if (diag == NULL) {
        return;
    }
    diag->lastSocketWriteAt = now;
}

static inline void stratum_session_diag_on_pool_message(stratum_session_diag_t *diag, uint32_t now) {
    if (diag == NULL) {
        return;
    }
    diag->lastPoolMessageAt = now;
}

static inline void stratum_session_diag_on_notify(
    stratum_session_diag_t *diag,
    uint32_t now,
    const char *jobId,
    const char *prevHash,
    bool cleanJobs
) {
    if (diag == NULL) {
        return;
    }
    diag->lastNotifyAt = now;
    diag->lastPoolMessageAt = now;
    diag->lastNotifyCleanJobs = cleanJobs;
    if (cleanJobs) {
        diag->hasSeenCleanJobs = true;
        diag->lastCleanJobsAt = now;
    }
    diag->waitingForWorkAfterAcceptedBlock = false;
    diag->waitingForWorkSinceAt = 0;
    diag->lastWaitWarningAt = 0;
    stratum_diag_copy(diag->currentJobId, jobId, sizeof(diag->currentJobId));
    stratum_diag_copy(diag->currentPrevHash, prevHash, sizeof(diag->currentPrevHash));
}

static inline void stratum_session_diag_on_submit(
    stratum_session_diag_t *diag,
    uint32_t now,
    const char *jobId,
    bool isBlockCandidate
) {
    if (diag == NULL) {
        return;
    }
    diag->lastSubmitAt = now;
    diag->lastSubmitWasBlock = isBlockCandidate;
    stratum_diag_copy(diag->lastSubmitJobId, jobId, sizeof(diag->lastSubmitJobId));
}

static inline void stratum_session_diag_on_submit_result(
    stratum_session_diag_t *diag,
    uint32_t now,
    bool accepted,
    bool wasBlockCandidate
) {
    if (diag == NULL) {
        return;
    }
    diag->lastPoolMessageAt = now;
    if (!accepted) {
        return;
    }
    diag->lastAcceptedAt = now;
    if (wasBlockCandidate) {
        diag->lastAcceptedBlockAt = now;
        diag->waitingForWorkAfterAcceptedBlock = true;
        diag->waitingForWorkSinceAt = now;
        diag->lastWaitWarningAt = 0;
    }
}

static inline void stratum_session_diag_on_reconnect_request(stratum_session_diag_t *diag, uint32_t now) {
    if (diag == NULL) {
        return;
    }
    diag->lastReconnectRequestAt = now;
}

static inline void stratum_session_diag_on_extranonce(
    stratum_session_diag_t *diag,
    const char *extraNonce1,
    int extraNonce2Size
) {
    if (diag == NULL) {
        return;
    }
    stratum_diag_copy(diag->extraNonce1, extraNonce1, sizeof(diag->extraNonce1));
    diag->extraNonce2Size = extraNonce2Size;
}

static inline uint32_t stratum_session_diag_age(uint32_t now, uint32_t timestamp) {
    return timestamp == 0 ? 0 : (uint32_t)(now - timestamp);
}

static inline bool stratum_session_diag_should_warn_waiting_for_work(
    stratum_session_diag_t *diag,
    uint32_t now,
    uint32_t warningMs
) {
    if (diag == NULL || !diag->waitingForWorkAfterAcceptedBlock || diag->waitingForWorkSinceAt == 0) {
        return false;
    }
    if (stratum_session_diag_age(now, diag->waitingForWorkSinceAt) < warningMs) {
        return false;
    }
    if (diag->lastWaitWarningAt != 0 && stratum_session_diag_age(now, diag->lastWaitWarningAt) < warningMs) {
        return false;
    }
    diag->lastWaitWarningAt = now;
    return true;
}

static inline bool stratum_session_diag_should_disconnect_for_inactivity(
    const stratum_session_diag_t *diag,
    uint32_t now,
    uint32_t inactivityMs
) {
    if (diag == NULL || diag->lastPoolMessageAt == 0) {
        return false;
    }
    return (uint32_t)(now - diag->lastPoolMessageAt) > inactivityMs;
}

#endif