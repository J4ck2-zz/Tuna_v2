package tunav2

import "Tuna_v2/dag"

const WaveLength dag.Round = 3

func VotingRound(round dag.Round) dag.Round {
	return round + 1
}

func DecisionRound(round dag.Round) dag.Round {
	return round + WaveLength - 1
}
