#include <assert.h>
#include <iostream>
#include <string>
#include <vector>

#include "stratum/stratum_line_buffer.h"

static std::vector<std::string> feedChunks(StratumLineBuffer &buf, const std::vector<std::string> &chunks) {
    std::vector<std::string> lines;
    std::string line;
    bool dropped = false;

    for (const auto &chunk : chunks) {
        for (char c : chunk) {
            if (buf.push(c, line, dropped)) {
                lines.push_back(line);
            }
        }
    }

    return lines;
}

void test_multiple_json_messages_in_one_tcp_read() {
    StratumLineBuffer buf;

    std::vector<std::string> chunks = {
        "{\"method\":\"mining.notify\"}\n{\"id\":2,\"result\":true}\n{\"method\":\"mining.set_difficulty\",\"params\":[64]}\n"
    };

    std::vector<std::string> lines = feedChunks(buf, chunks);
    assert(lines.size() == 3);
    assert(lines[0] == "{\"method\":\"mining.notify\"}");
    assert(lines[1] == "{\"id\":2,\"result\":true}");
    assert(lines[2] == "{\"method\":\"mining.set_difficulty\",\"params\":[64]}");
}

void test_split_json_message_across_tcp_reads() {
    StratumLineBuffer buf;

    std::vector<std::string> chunks = {
        "{\"method\":\"mining.not",
        "ify\",\"params\":[\"job-1\"]}",
        "\n"
    };

    std::vector<std::string> lines = feedChunks(buf, chunks);
    assert(lines.size() == 1);
    assert(lines[0] == "{\"method\":\"mining.notify\",\"params\":[\"job-1\"]}");
}

void test_notify_before_authorize_response() {
    StratumLineBuffer buf;

    std::vector<std::string> chunks = {
        "{\"method\":\"mining.notify\",\"params\":[\"job-a\"]}\n{\"id\":2,\"result\":true}\n"
    };

    std::vector<std::string> lines = feedChunks(buf, chunks);
    assert(lines.size() == 2);
    assert(lines[0].find("\"method\":\"mining.notify\"") != std::string::npos);
    assert(lines[1].find("\"id\":2") != std::string::npos);
}

void test_set_difficulty_before_subscribe_response() {
    StratumLineBuffer buf;

    std::vector<std::string> chunks = {
        "{\"method\":\"mining.set_difficulty\",\"params\":[32]}\n{\"id\":1,\"result\":[[],\"abcd\",4]}\n"
    };

    std::vector<std::string> lines = feedChunks(buf, chunks);
    assert(lines.size() == 2);
    assert(lines[0].find("\"method\":\"mining.set_difficulty\"") != std::string::npos);
    assert(lines[1].find("\"id\":1") != std::string::npos);
}

void test_authorize_response_before_subscribe_response() {
    StratumLineBuffer buf;

    std::vector<std::string> chunks = {
        "{\"id\":2,\"result\":true}\n{\"id\":1,\"result\":[[],\"abcd\",4]}\n"
    };

    std::vector<std::string> lines = feedChunks(buf, chunks);
    assert(lines.size() == 2);
    assert(lines[0].find("\"id\":2") != std::string::npos);
    assert(lines[1].find("\"id\":1") != std::string::npos);
}

int main(int argc, char **argv) {
    (void)argc;
    (void)argv;

    test_multiple_json_messages_in_one_tcp_read();
    test_split_json_message_across_tcp_reads();
    test_notify_before_authorize_response();
    test_set_difficulty_before_subscribe_response();
    test_authorize_response_before_subscribe_response();

    std::cout << "All Stratum line-buffer tests passed.\n";
    return 0;
}
