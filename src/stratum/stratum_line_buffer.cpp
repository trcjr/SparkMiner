#include "stratum_line_buffer.h"

StratumLineBuffer::StratumLineBuffer(size_t maxLineLen)
    : m_maxLineLen(maxLineLen), m_current(), m_droppingLongLine(false) {
    m_current.reserve(256);
}

bool StratumLineBuffer::push(char c, std::string &outLine, bool &outDroppedLongLine) {
    outDroppedLongLine = false;

    if (c == '\r') {
        return false;
    }

    if (c == '\n') {
        if (m_droppingLongLine) {
            m_droppingLongLine = false;
            m_current.clear();
            outDroppedLongLine = true;
            return false;
        }

        outLine = m_current;
        m_current.clear();
        return true;
    }

    if (m_droppingLongLine) {
        return false;
    }

    if (m_current.length() >= m_maxLineLen) {
        m_current.clear();
        m_droppingLongLine = true;
        return false;
    }

    m_current.push_back(c);
    return false;
}

void StratumLineBuffer::reset() {
    m_current.clear();
    m_droppingLongLine = false;
}
