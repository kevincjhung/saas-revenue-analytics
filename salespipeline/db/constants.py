# Define the probabilities/distributions for the enums
from salespipeline.db.enums import OpportunityStage


STAGE_PROBABILITIES = {
    OpportunityStage.PROSPECTING: 0.10,   # 10% chance to close
    OpportunityStage.QUALIFIED: 0.25,
    OpportunityStage.PROPOSAL: 0.40,
    OpportunityStage.NEGOTIATION: 0.60,
    OpportunityStage.CLOSED_WON: 1.00,
    OpportunityStage.CLOSED_LOST: 0.00,
}
