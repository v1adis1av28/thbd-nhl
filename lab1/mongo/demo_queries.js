// Протоколы с голами. typeDescKey лежит внутри массива plays.
db.games.find(
  { "plays.typeDescKey": "goal" },
  { game_id: 1, game_date: 1, home: 1, away: 1, goal_count: 1 }
).sort({ game_date: -1 }).limit(5)

// Кто забивал чаще в загруженном окне.
db.games.aggregate([
  { $unwind: "$plays" },
  { $match: { "plays.typeDescKey": "goal" } },
  { $group: { _id: "$plays.details.scoringPlayerId", goals: { $sum: 1 } } },
  { $sort: { goals: -1 } },
  { $limit: 10 }
])
