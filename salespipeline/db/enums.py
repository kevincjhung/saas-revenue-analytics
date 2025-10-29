from enum import Enum

class OpportunityStage(str, Enum):
    PROSPECTING = "prospecting"
    QUALIFIED = "qualified"
    PROPOSAL = "proposal"
    NEGOTIATION = "negotiation"
    CLOSED_WON = "closed_won"
    CLOSED_LOST = "closed_lost"
    