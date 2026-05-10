# Solution

This folder contains the shortest reviewer-facing version of the final answers.

For the formal submission files requested in the assignment, see the repo-root `exercise_2_answer.tsv` and `exercise_3_answer.tsv`.
For the interview submission slide, see `developer_setup_one_slide.md`.

If you want the full reasoning path, the recommended notebook order is:

1. `notebooks/00_setup_and_load_data.ipynb`
2. `notebooks/01_data_eda_and_assumptions.ipynb`
3. `notebooks/02_exercise_2_walkthrough.ipynb`
4. `notebooks/03_exercise_2_production_solution.ipynb`
5. `notebooks/04_exercise_3_walkthrough.ipynb`
6. `notebooks/05_exercise_3_production_solution.ipynb`

## Exercise 2

Question:

What are the top 10 songs played in the top 50 longest sessions by track count?

Answer file:

- `exercise_2_answer.tsv`

Top result:

1. Cake - Jolene: 1214 plays
2. The Knife - Heartbeats: 868 plays
3. Jeff Buckley & Gary Lucas - How Long Will It Take: 726 plays
4. Broken Social Scene - Anthems For A Seventeen Year Old Girl: 659 plays
5. Elliott Smith - St. Ides Heaven: 646 plays
6. The Killers - Bonus Track: 634 plays
7. 2Pac - Starin' Through My Rear View: 617 plays
8. The Rolling Stones - Beast Of Burden: 613 plays
9. Everclear - The Swing: 604 plays
10. Kanye West - See You In My Nightmares: 536 plays

## Exercise 3

Selected metric:

- Number of sessions per month

Top user:

- `user_000833`
- Total sessions: `6897`

Forecast file:

- `exercise_3_forecast.tsv`

Forecast for the next 3 months after the last available month:

1. 2009-06-01: 163 sessions
2. 2009-07-01: 192 sessions
3. 2009-08-01: 212 sessions
