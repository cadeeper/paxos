import json

MSG_PREPARE = 1
MSG_ACCEPT = 2
MSG_PROMISE = 3
MSG_ACCEPTED = 4
MSG_START = 200

type_map = {
    MSG_PREPARE: "prepare",
    MSG_ACCEPT: "accept",
    MSG_PROMISE: "promise",
    MSG_ACCEPTED: "accepted",
    MSG_START: "start",
}


class Message:
    type = None
    uid = None
    proposal_id = None
    proposal_value = None
    accepted_id = None
    accepted_value = None

    def __init__(self, type, uid=None, proposal_id=None, proposal_value=None, accepted_id=None,
        accepted_value=None):
        self.type = type
        self.uid = uid
        self.proposal_id = proposal_id
        self.proposal_value = proposal_value
        self.accepted_id = accepted_id
        self.accepted_value = accepted_value

    def type_name(self):
        return type_map[self.type]

    def __str__(self):
        return "消息类型:{0} 消息来源:{1} proposal_id:{2} proposal_value:{3} accepted_id:{4} accepted_value:{5}". \
            format(self.type_name(), self.uid, self.proposal_id, self.proposal_value, self.accepted_id,
                   self.accepted_value)

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__,
                          sort_keys=True, indent=4)
