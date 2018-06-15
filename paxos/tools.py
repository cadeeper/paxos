

class ProposalID:
    _id = 0
    def __init__(self):
        pass

    @staticmethod
    def incr_proposal_id():
        ProposalID._id = ProposalID._id + 1
        return ProposalID._id



if __name__ == "__main__":
    p1 = ProposalID()
    print(p1.incr_proposal_id())
    p2 = ProposalID()
    print(p2.incr_proposal_id())