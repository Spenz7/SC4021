# Question 4 and 5 Summary

## Q4

- Verified evaluation set: `3.3/eval1kFinal_hf_sentiment.xlsx` with 1,000 labeled comments.
- Reference legacy baseline from `3.1new/3.1Final/evalPersonalCrawl.xlsx`:
  - Accuracy: 0.3370
  - Macro-F1: 0.3206
- Q4 classifier used for the main evaluation: `text_unigram_logreg` (5-fold stratified cross-validation on the verified 1k set).
  - Accuracy: 0.6340
  - Macro-F1: 0.4674
  - Weighted-F1: 0.6213
  - Mean prediction throughput during CV inference: 13479.55 records/sec

### Q4 Per-Class Metrics

| Class | Precision | Recall | F1 |
|---|---:|---:|---:|
| NEGATIVE | 0.6801 | 0.7193 | 0.6992 |
| NEUTRAL | 0.5813 | 0.6030 | 0.5920 |
| POSITIVE | 0.3077 | 0.0678 | 0.1111 |

### Q4 Random Accuracy Test Preparation

- Remaining cleaned corpus rows after excluding the verified 1k: 10354
- Fitted the Q4 text-unigram classifier on the full verified 1k, then predicted the remaining corpus.
- Prediction throughput on the remaining corpus: 33789.91 records/sec
- Predicted label distribution on the remaining corpus: {'NEGATIVE': 7155, 'NEUTRAL': 2581, 'POSITIVE': 618}
- Random audit workbook created at `3.3/q4_random_audit_sample.xlsx` with 100 sampled comments and blank manual-review columns.

## Q5

Q5 explores two incremental enhancements over the Q4 unigram baseline:

1. Add the existing polarity and subjectivity scores as extra numeric features (`text_unigram_plus_numeric`).
2. Add word bigrams on top of the text+numeric hybrid (`text_bigram_plus_numeric`).

### Q5 Ablation

| Model | Accuracy | Macro-F1 | Weighted-F1 |
|---|---:|---:|---:|
| legacy_raw_labels | 0.3370 | 0.3206 | 0.3829 |
| polarity_band_tuned | 0.3910 | 0.3269 | 0.3553 |
| hybrid_gate_tuned | 0.4030 | 0.3462 | 0.4002 |
| text_unigram_logreg | 0.6340 | 0.4674 | 0.6213 |
| text_unigram_plus_numeric | 0.6320 | 0.5260 | 0.6349 |
| text_bigram_plus_numeric | 0.6430 | 0.5244 | 0.6422 |

Recommended final enhancement for the report: `text_unigram_plus_numeric`.
It produced the best macro-F1, which is the better choice here because the verified dataset is imbalanced toward `NEGATIVE` and `NEUTRAL`.
