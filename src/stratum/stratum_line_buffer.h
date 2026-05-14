#ifndef STRATUM_LINE_BUFFER_H
#define STRATUM_LINE_BUFFER_H

#include <stddef.h>
#include <string>

class StratumLineBuffer {
public:
    explicit StratumLineBuffer(size_t maxLineLen = 4096);

    // Push one byte. Returns true when a complete line is ready in outLine.
    // If a line exceeds maxLineLen, bytes are discarded until newline and
    // outDroppedLongLine is set when that newline arrives.
    bool push(char c, std::string &outLine, bool &outDroppedLongLine);

    void reset();

private:
    size_t m_maxLineLen;
    std::string m_current;
    bool m_droppingLongLine;
};

#endif // STRATUM_LINE_BUFFER_H
