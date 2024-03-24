from app.entities.agent_data import AgentData
from app.entities.processed_agent_data import ProcessedAgentData
import datetime


def process_agent_data(
    agent_data: AgentData,
    user_id: int,
    timestamp: datetime
) -> ProcessedAgentData:
    """
    Process agent data and classify the state of the road surface.
    Parameters:
        agent_data (AgentData): Agent data that containing accelerometer, GPS, and timestamp.
    Returns:
        processed_data_batch (ProcessedAgentData): Processed data containing the classified state of the road surface and agent data.
    """
    road_state = 'good'

    if agent_data.accelerometer.y < -100 or agent_data.accelerometer.y > 100:
        road_state = 'bad'

    return ProcessedAgentData(
        road_state=road_state,
        agent_data=agent_data,
        user_id=user_id,
        timestamp=timestamp,
    )
