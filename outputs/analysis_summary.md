# Coding Challenge - Exercise 2 Summary

## Dataset

- Raw input file: `C:\Users\GonzaloFigueroa\Documents\Private\BME\coding challenge\data\raw\lastfm-dataset-1K\userid-timestamp-artid-artname-traid-traname.tsv`
- Prepared Parquet dataset: `C:\Users\GonzaloFigueroa\Documents\Private\BME\coding challenge\data\processed\lastfm_events_parquet`
- Total valid rows loaded: `19150867`
- Distinct users: `992`

## Question

What are the top 10 songs played in the top 50 longest sessions by track count?

## Answer

1. Cake - Jolene (plays=1214, sessions=12)
2. The Knife - Heartbeats (plays=868, sessions=2)
3. Jeff Buckley & Gary Lucas - How Long Will It Take (plays=726, sessions=2)
4. Broken Social Scene - Anthems For A Seventeen Year Old Girl (plays=659, sessions=6)
5. Elliott Smith - St. Ides Heaven (plays=646, sessions=6)
6. The Killers - Bonus Track (plays=634, sessions=12)
7. 2Pac - Starin' Through My Rear View (plays=617, sessions=12)
8. The Rolling Stones - Beast Of Burden (plays=613, sessions=3)
9. Everclear - The Swing (plays=604, sessions=15)
10. Kanye West - See You In My Nightmares (plays=536, sessions=3)

## Output files

- `exercise_2_answer.tsv`
- `exercise_2_answer.tsv`
- `outputs/exercise_2_answer.tsv`
- `top_10_songs_in_top_50_sessions.tsv`
- `top_50_longest_sessions.tsv`
