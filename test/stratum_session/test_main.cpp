#include <assert.h>
#include <iostream>

#include "stratum/stratum_session_diag.h"

static void test_accepted_block_with_delayed_notify_stays_connected() {
    stratum_session_diag_t diag;
    stratum_session_diag_reset(&diag);

    stratum_session_diag_on_socket_connected(&diag, 1000);
    stratum_session_diag_on_notify(&diag, 1500, "job-1", "11", false);
    stratum_session_diag_on_submit(&diag, 2000, "job-1", true);
    stratum_session_diag_on_submit_result(&diag, 2100, true, true);

    assert(diag.waitingForWorkAfterAcceptedBlock);
    assert(!stratum_session_diag_should_disconnect_for_inactivity(&diag, 68000, 700000));
    assert(stratum_session_diag_should_warn_waiting_for_work(&diag, 33000, 30000));

    stratum_session_diag_on_notify(&diag, 68100, "job-2", "22", true);
    assert(!diag.waitingForWorkAfterAcceptedBlock);
    assert(diag.lastNotifyCleanJobs);
    assert(diag.hasSeenCleanJobs);
}

static void test_accepted_block_without_followup_notify_does_not_trigger_fast_disconnect() {
    stratum_session_diag_t diag;
    stratum_session_diag_reset(&diag);

    stratum_session_diag_on_socket_connected(&diag, 1000);
    stratum_session_diag_on_notify(&diag, 1500, "job-1", "11", false);
    stratum_session_diag_on_submit(&diag, 2000, "job-1", true);
    stratum_session_diag_on_submit_result(&diag, 2100, true, true);

    assert(!stratum_session_diag_should_disconnect_for_inactivity(&diag, 70000, 700000));
    assert(!stratum_session_diag_should_disconnect_for_inactivity(&diag, 300000, 700000));
    assert(!stratum_session_diag_should_disconnect_for_inactivity(&diag, 702000, 700000));
    assert(stratum_session_diag_should_disconnect_for_inactivity(&diag, 703000, 700000));
}

static void test_followup_notify_without_clean_jobs_still_replaces_work() {
    stratum_session_diag_t diag;
    stratum_session_diag_reset(&diag);

    stratum_session_diag_on_socket_connected(&diag, 1000);
    stratum_session_diag_on_notify(&diag, 1500, "job-1", "11", false);
    stratum_session_diag_on_submit(&diag, 2000, "job-1", true);
    stratum_session_diag_on_submit_result(&diag, 2100, true, true);
    stratum_session_diag_on_notify(&diag, 70000, "job-2", "22", false);

    assert(!diag.waitingForWorkAfterAcceptedBlock);
    assert(!diag.lastNotifyCleanJobs);
    assert(std::string(diag.currentJobId) == "job-2");
    assert(std::string(diag.currentPrevHash) == "22");
}

static void test_extranonce_update_does_not_force_reconnect() {
    stratum_session_diag_t diag;
    stratum_session_diag_reset(&diag);

    stratum_session_diag_on_socket_connected(&diag, 1000);
    stratum_session_diag_on_extranonce(&diag, "abcd1234", 4);
    stratum_session_diag_on_pool_message(&diag, 1500);

    assert(std::string(diag.extraNonce1) == "abcd1234");
    assert(diag.extraNonce2Size == 4);
    assert(!stratum_session_diag_should_disconnect_for_inactivity(&diag, 61000, 700000));
}

int main() {
    test_accepted_block_with_delayed_notify_stays_connected();
    test_accepted_block_without_followup_notify_does_not_trigger_fast_disconnect();
    test_followup_notify_without_clean_jobs_still_replaces_work();
    test_extranonce_update_does_not_force_reconnect();

    std::cout << "All Stratum session tests passed.\n";
    return 0;
}