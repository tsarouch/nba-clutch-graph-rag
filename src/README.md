# NBA Clutch Moments with Graph-RAG

This project shows how **Graph-RAG** can retrieve and narrate clutch moments 
from NBA play-by-play data — inspired by *The Last Dance* and the Bulls' golden years.  

By combining a **Graph database** (for structure and facts) with **LLMs** (for narration and context), 
we can surface iconic moments such as Steve Kerr’s game-winning shot in the 1997 NBA Finals.

---

## 🔍 Example Query

**Plain English**
> "Give me the games where, in the last 30 seconds, someone scored and turned it into a victory."

**Generated Cypher**
```cypher
MATCH (e:Event {is_clutch: true, event_type: 1})-[:IN_GAME]->(g:Game)
MATCH (scorer:Player)-[:PERFORMED {role:"PLAYER1_ID"}]->(e)
RETURN g.game_id AS game,
       e.period AS period,
       e.seconds_left_period AS sec_left,
       scorer.name AS scorer,
       e.score AS score,
       e.score_margin AS margin,
       coalesce(e.home_desc, e.visit_desc) AS desc
ORDER BY g.game_id, sec_left
