import sqlite3, os

EVAL_DB = '/home/diana.z/hack/llm_judge/data/evaluations.db'
PAPERS_DB = '/home/diana.z/hack/download_papers_pubmed/paper_collection/data/papers.db'

# 1) Candidate DOIs from evaluations.db
ev = sqlite3.connect(EVAL_DB)
ev.row_factory = lambda c, r: r[0]
dois_ev = set(ev.execute("""
  SELECT DISTINCT doi
  FROM paper_evaluations
  WHERE doi IS NOT NULL AND doi != ''
    AND (
      result IN ('valid','doubted')
      OR (result='not_valid' AND COALESCE(confidence_score,999) <= 7)
    )
""").fetchall())
ev.close()

# 2) Filter by papers.db (missing abstract OR missing full text sections)
pa = sqlite3.connect(PAPERS_DB)
pa.row_factory = lambda c, r: r[0]
dois_need = set(pa.execute("""
  SELECT p.doi
  FROM papers p
  WHERE p.doi IS NOT NULL AND p.doi != ''
    AND (
      p.abstract IS NULL OR p.abstract = ''
      OR p.full_text_sections IS NULL OR p.full_text_sections = ''
    )
""").fetchall())
pa.close()

final = sorted(dois_ev & dois_need)

out = 'missing_dois/dois_to_process.txt'
with open(out, 'w', encoding='utf-8') as f:
    for d in final:
        f.write(d + '\n')

print(f"Wrote {len(final)} DOIs to {out}")
