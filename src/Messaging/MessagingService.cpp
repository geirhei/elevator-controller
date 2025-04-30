#include "MessagingService.hpp"

#include <zyre.h>

#include <utility>

namespace Messaging {

Message::Message()
    : Message { {} }
{
}

Message::Message(Payload payload)
    : Message { generateUuid(), std::move(payload) }
{
}

Message::Message(Uuid uuid, Payload payload)
    : m_uuid { std::move(uuid) }
    , m_payload { std::move(payload) }
{
}

Message::Uuid Message::uuid() const
{
    return m_uuid;
}

Message::Payload& Message::payload()
{
    return m_payload;
}

const Message::Payload& Message::payload() const
{
    return m_payload;
}

Message::Uuid Message::generateUuid()
{
    zuuid_t* zuuid = zuuid_new();
    const Uuid uuid { zuuid_str(zuuid) };
    zuuid_destroy(&zuuid);
    return uuid;
}

}
