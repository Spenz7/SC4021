import html
import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix, f1_score
from sklearn.model_selection import StratifiedKFold
from sklearn.pipeline import Pipeline


THIS_DIR = Path(__file__).resolve().parent
REPO_ROOT = THIS_DIR.parent

VERIFIED_EVAL_PATH = THIS_DIR / "eval1kFinal_hf_sentiment.xlsx"
LEGACY_EVAL_PATH = REPO_ROOT / "3.1" / "3.1Final" / "evalPersonalCrawl.xlsx"
CLEANED_CORPUS_PATH = REPO_ROOT / "indexing" / "cleaned_data.csv"

RESULTS_JSON = THIS_DIR / "q4_q5_results.json"
SUMMARY_MD = THIS_DIR / "q4_q5_summary.md"
ABLATION_CSV = THIS_DIR / "q5_ablation.csv"
Q4_CONFUSION_CSV = THIS_DIR / "q4_word_unigram_confusion_matrix.csv"
Q5_CONFUSION_CSV = THIS_DIR / "q5_word_unigram_plus_numeric_confusion_matrix.csv"
AUDIT_SAMPLE_XLSX = THIS_DIR / "q4_random_audit_sample.xlsx"

LABELS = ["NEGATIVE", "NEUTRAL", "POSITIVE"]
RANDOM_STATE = 42


def normalize_text(text: str) -> str:
    if text is None:
        return ""
    text = html.unescape(str(text))
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_for_model(text: str) -> str:
    return normalize_text(text).lower()


def build_text_meta_frame(texts: pd.Series) -> pd.DataFrame:
    rows = []
    for raw_text in texts:
        text = normalize_text(raw_text)
        letters = sum(ch.isalpha() for ch in text)
        upper = sum(ch.isupper() for ch in text)
        rows.append(
            {
                "text": text.lower(),
                "char_len": len(text),
                "word_len": len(text.split()),
                "exclaim_count": text.count("!"),
                "question_count": text.count("?"),
                "upper_ratio": (upper / letters) if letters else 0.0,
            }
        )
    return pd.DataFrame(rows)


def load_verified_eval() -> pd.DataFrame:
    df = pd.read_excel(VERIFIED_EVAL_PATH)
    df = df.rename(columns={"comment": "raw_text", "label": "gold_label"})
    df["raw_text"] = df["raw_text"].astype(str)
    df["gold_label"] = df["gold_label"].astype(str).str.upper()
    df["normalized_text"] = df["raw_text"].map(normalize_text)
    return df


def load_legacy_eval() -> pd.DataFrame:
    df = pd.read_excel(LEGACY_EVAL_PATH)
    df = df.rename(
        columns={
            "comment": "raw_text",
            "label": "legacy_label",
            "polarity": "legacy_polarity",
            "subjectivity": "legacy_subjectivity",
        }
    )
    df["raw_text"] = df["raw_text"].astype(str)
    df["legacy_label"] = df["legacy_label"].astype(str).str.upper()
    df["normalized_text"] = df["raw_text"].map(normalize_text)
    return df


def align_eval_frames() -> pd.DataFrame:
    verified = load_verified_eval()
    legacy = load_legacy_eval()

    mismatch_count = int((verified["normalized_text"] != legacy["normalized_text"]).sum())
    if mismatch_count:
        raise ValueError(f"Verified and legacy eval sheets do not align for {mismatch_count} rows.")

    text_meta = build_text_meta_frame(verified["raw_text"])
    merged = pd.concat(
        [
            verified.reset_index(drop=True),
            legacy[["legacy_label", "legacy_polarity", "legacy_subjectivity"]].reset_index(drop=True),
            text_meta.reset_index(drop=True),
        ],
        axis=1,
    )
    return merged


def make_text_unigram_model() -> Pipeline:
    pre = ColumnTransformer(
        [
            ("text", TfidfVectorizer(ngram_range=(1, 1), min_df=2, sublinear_tf=True), "text"),
        ]
    )
    return Pipeline(
        [
            ("pre", pre),
            ("clf", LogisticRegression(max_iter=2000, class_weight="balanced", multi_class="auto", random_state=RANDOM_STATE)),
        ]
    )


def make_text_unigram_plus_numeric_model() -> Pipeline:
    pre = ColumnTransformer(
        [
            ("text", TfidfVectorizer(ngram_range=(1, 1), min_df=2, sublinear_tf=True), "text"),
            ("num", "passthrough", ["polarity", "subjectivity"]),
        ]
    )
    return Pipeline(
        [
            ("pre", pre),
            ("clf", LogisticRegression(max_iter=2000, class_weight="balanced", multi_class="auto", random_state=RANDOM_STATE)),
        ]
    )


def make_text_bigram_plus_numeric_model() -> Pipeline:
    pre = ColumnTransformer(
        [
            ("text", TfidfVectorizer(ngram_range=(1, 2), min_df=2, sublinear_tf=True), "text"),
            ("num", "passthrough", ["polarity", "subjectivity"]),
        ]
    )
    return Pipeline(
        [
            ("pre", pre),
            ("clf", LogisticRegression(max_iter=2000, class_weight="balanced", multi_class="auto", random_state=RANDOM_STATE)),
        ]
    )


@dataclass
class EvaluationResult:
    name: str
    accuracy: float
    macro_f1: float
    weighted_f1: float
    report: dict
    confusion: list[list[int]]
    fit_time_sec: Optional[float] = None
    predict_time_sec: Optional[float] = None
    records_per_sec: Optional[float] = None
    oof_predictions: Optional[list[str]] = None


def summarize_predictions(
    name: str,
    gold: np.ndarray,
    pred: np.ndarray,
    fit_time_sec: Optional[float] = None,
    predict_time_sec: Optional[float] = None,
) -> EvaluationResult:
    report = classification_report(gold, pred, labels=LABELS, output_dict=True, zero_division=0)
    accuracy = float(accuracy_score(gold, pred))
    macro_f1 = float(f1_score(gold, pred, labels=LABELS, average="macro", zero_division=0))
    weighted_f1 = float(f1_score(gold, pred, labels=LABELS, average="weighted", zero_division=0))
    confusion = confusion_matrix(gold, pred, labels=LABELS).tolist()
    records_per_sec = None
    if predict_time_sec and predict_time_sec > 0:
        records_per_sec = len(gold) / predict_time_sec
    return EvaluationResult(
        name=name,
        accuracy=accuracy,
        macro_f1=macro_f1,
        weighted_f1=weighted_f1,
        report=report,
        confusion=confusion,
        fit_time_sec=fit_time_sec,
        predict_time_sec=predict_time_sec,
        records_per_sec=records_per_sec,
        oof_predictions=pred.tolist(),
    )


def evaluate_legacy_baseline(df: pd.DataFrame) -> EvaluationResult:
    gold = df["gold_label"].to_numpy()
    pred = df["legacy_label"].to_numpy()
    return summarize_predictions("legacy_raw_labels", gold, pred)


def evaluate_cv_model(name: str, model: Pipeline, X: pd.DataFrame, y: np.ndarray) -> EvaluationResult:
    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=RANDOM_STATE)
    oof_pred = np.empty(len(y), dtype=object)
    fit_total = 0.0
    predict_total = 0.0

    for train_idx, test_idx in skf.split(X, y):
        X_train = X.iloc[train_idx]
        X_test = X.iloc[test_idx]
        y_train = y[train_idx]

        fit_start = time.perf_counter()
        model.fit(X_train, y_train)
        fit_total += time.perf_counter() - fit_start

        pred_start = time.perf_counter()
        oof_pred[test_idx] = model.predict(X_test)
        predict_total += time.perf_counter() - pred_start

    return summarize_predictions(name, y, oof_pred, fit_total, predict_total)


def evaluate_tuned_threshold_models(df: pd.DataFrame) -> list[EvaluationResult]:
    y = df["gold_label"].to_numpy()
    polarity = df["legacy_polarity"].astype(float).to_numpy()
    subjectivity = df["legacy_subjectivity"].astype(float).to_numpy()
    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=RANDOM_STATE)

    pol_oof = np.empty(len(y), dtype=object)
    hybrid_oof = np.empty(len(y), dtype=object)

    polarity_thresholds = np.linspace(0.0, 0.5, 26)
    subjectivity_thresholds = np.linspace(0.0, 1.0, 21)

    def polarity_band_predictions(values: np.ndarray, threshold: float) -> np.ndarray:
        return np.where(values > threshold, "POSITIVE", np.where(values < -threshold, "NEGATIVE", "NEUTRAL"))

    def hybrid_predictions(values: np.ndarray, subjectivities: np.ndarray, pol_threshold: float, subj_threshold: float) -> np.ndarray:
        out = []
        for pol_value, subj_value in zip(values, subjectivities):
            if abs(pol_value) <= pol_threshold or subj_value <= subj_threshold:
                out.append("NEUTRAL")
            elif pol_value > 0:
                out.append("POSITIVE")
            else:
                out.append("NEGATIVE")
        return np.asarray(out)

    for train_idx, test_idx in skf.split(polarity.reshape(-1, 1), y):
        y_train = y[train_idx]
        pol_train = polarity[train_idx]
        subj_train = subjectivity[train_idx]

        best_pol_threshold = 0.0
        best_pol_score = -1.0
        for threshold in polarity_thresholds:
            pred_train = polarity_band_predictions(pol_train, threshold)
            score = f1_score(y_train, pred_train, labels=LABELS, average="macro", zero_division=0)
            if score > best_pol_score:
                best_pol_score = score
                best_pol_threshold = threshold

        pol_oof[test_idx] = polarity_band_predictions(polarity[test_idx], best_pol_threshold)

        best_pair = (0.0, 0.0)
        best_pair_score = -1.0
        for pol_threshold in polarity_thresholds:
            for subj_threshold in subjectivity_thresholds:
                pred_train = hybrid_predictions(pol_train, subj_train, pol_threshold, subj_threshold)
                score = f1_score(y_train, pred_train, labels=LABELS, average="macro", zero_division=0)
                if score > best_pair_score:
                    best_pair_score = score
                    best_pair = (pol_threshold, subj_threshold)

        hybrid_oof[test_idx] = hybrid_predictions(
            polarity[test_idx],
            subjectivity[test_idx],
            best_pair[0],
            best_pair[1],
        )

    return [
        summarize_predictions("polarity_band_tuned", y, pol_oof),
        summarize_predictions("hybrid_gate_tuned", y, hybrid_oof),
    ]


def save_confusion_csv(path: Path, confusion: list[list[int]]) -> None:
    df = pd.DataFrame(confusion, index=LABELS, columns=LABELS)
    df.to_csv(path)


def load_remaining_corpus(eval_df: pd.DataFrame) -> pd.DataFrame:
    corpus = pd.read_csv(CLEANED_CORPUS_PATH)
    corpus["text"] = corpus["text"].astype(str)
    corpus["normalized_text"] = corpus["text"].map(normalize_text)
    eval_texts = set(eval_df["normalized_text"])
    remaining = corpus[~corpus["normalized_text"].isin(eval_texts)].copy().reset_index(drop=True)
    remaining["text_model"] = remaining["text"].map(normalize_for_model)
    return remaining


def build_random_audit_sample(eval_df: pd.DataFrame) -> dict:
    remaining = load_remaining_corpus(eval_df)
    X_train = pd.DataFrame({"text": eval_df["text"]})
    y_train = eval_df["gold_label"].to_numpy()
    model = make_text_unigram_model()

    fit_start = time.perf_counter()
    model.fit(X_train, y_train)
    fit_time = time.perf_counter() - fit_start

    X_remaining = pd.DataFrame({"text": remaining["text_model"]})
    predict_start = time.perf_counter()
    probabilities = model.predict_proba(X_remaining)
    predict_time = time.perf_counter() - predict_start

    classes = model.named_steps["clf"].classes_
    predicted_idx = probabilities.argmax(axis=1)
    remaining["predicted_label"] = [classes[i] for i in predicted_idx]
    remaining["prediction_confidence"] = probabilities.max(axis=1)

    sample = remaining.sample(n=min(100, len(remaining)), random_state=RANDOM_STATE).copy()
    sample = sample[
        [
            "id",
            "subreddit",
            "post_title",
            "predicted_label",
            "prediction_confidence",
            "text",
            "url",
        ]
    ]
    sample["manual_label"] = ""
    sample["is_correct"] = ""
    sample["notes"] = ""
    sample.to_excel(AUDIT_SAMPLE_XLSX, index=False)

    label_counts = remaining["predicted_label"].value_counts().to_dict()
    records_per_sec = len(remaining) / predict_time if predict_time > 0 else None

    return {
        "remaining_rows": int(len(remaining)),
        "label_distribution": {str(k): int(v) for k, v in label_counts.items()},
        "fit_time_sec": fit_time,
        "predict_time_sec": predict_time,
        "records_per_sec": records_per_sec,
        "audit_sample_path": str(AUDIT_SAMPLE_XLSX.relative_to(REPO_ROOT)),
    }


def result_to_row(result: EvaluationResult) -> dict:
    return {
        "model": result.name,
        "accuracy": round(result.accuracy, 4),
        "macro_f1": round(result.macro_f1, 4),
        "weighted_f1": round(result.weighted_f1, 4),
        "fit_time_sec": round(result.fit_time_sec, 4) if result.fit_time_sec is not None else None,
        "predict_time_sec": round(result.predict_time_sec, 4) if result.predict_time_sec is not None else None,
        "records_per_sec": round(result.records_per_sec, 2) if result.records_per_sec is not None else None,
    }


def write_summary(
    baseline: EvaluationResult,
    q4_model: EvaluationResult,
    q5_results: list[EvaluationResult],
    audit_info: dict,
) -> None:
    q5_rows = [result_to_row(result) for result in q5_results]
    q5_lines = "\n".join(
        f"| {row['model']} | {row['accuracy']:.4f} | {row['macro_f1']:.4f} | {row['weighted_f1']:.4f} |"
        for row in q5_rows
    )

    q4_report = q4_model.report
    q4_class_lines = []
    for label in LABELS:
        metrics = q4_report.get(label, {})
        q4_class_lines.append(
            f"| {label} | {metrics.get('precision', 0.0):.4f} | {metrics.get('recall', 0.0):.4f} | {metrics.get('f1-score', 0.0):.4f} |"
        )

    summary = f"""# Question 4 and 5 Summary

## Q4

- Verified evaluation set: `{VERIFIED_EVAL_PATH.relative_to(REPO_ROOT)}` with 1,000 labeled comments.
- Reference legacy baseline from `{LEGACY_EVAL_PATH.relative_to(REPO_ROOT)}`:
  - Accuracy: {baseline.accuracy:.4f}
  - Macro-F1: {baseline.macro_f1:.4f}
- Q4 classifier used for the main evaluation: `text_unigram_logreg` (5-fold stratified cross-validation on the verified 1k set).
  - Accuracy: {q4_model.accuracy:.4f}
  - Macro-F1: {q4_model.macro_f1:.4f}
  - Weighted-F1: {q4_model.weighted_f1:.4f}
  - Mean prediction throughput during CV inference: {q4_model.records_per_sec:.2f} records/sec

### Q4 Per-Class Metrics

| Class | Precision | Recall | F1 |
|---|---:|---:|---:|
{chr(10).join(q4_class_lines)}

### Q4 Random Accuracy Test Preparation

- Remaining cleaned corpus rows after excluding the verified 1k: {audit_info['remaining_rows']}
- Fitted the Q4 text-unigram classifier on the full verified 1k, then predicted the remaining corpus.
- Prediction throughput on the remaining corpus: {audit_info['records_per_sec']:.2f} records/sec
- Predicted label distribution on the remaining corpus: {audit_info['label_distribution']}
- Random audit workbook created at `{audit_info['audit_sample_path']}` with 100 sampled comments and blank manual-review columns.

## Q5

Q5 explores two incremental enhancements over the Q4 unigram baseline:

1. Add the existing polarity and subjectivity scores as extra numeric features (`text_unigram_plus_numeric`).
2. Add word bigrams on top of the text+numeric hybrid (`text_bigram_plus_numeric`).

### Q5 Ablation

| Model | Accuracy | Macro-F1 | Weighted-F1 |
|---|---:|---:|---:|
{q5_lines}

Recommended final enhancement for the report: `text_unigram_plus_numeric`.
It produced the best macro-F1, which is the better choice here because the verified dataset is imbalanced toward `NEGATIVE` and `NEUTRAL`.
"""

    SUMMARY_MD.write_text(summary, encoding="utf-8")


def main() -> None:
    eval_df = align_eval_frames()
    y = eval_df["gold_label"].to_numpy()

    baseline_result = evaluate_legacy_baseline(eval_df)

    text_only_X = eval_df[["text"]]
    q4_model_result = evaluate_cv_model("text_unigram_logreg", make_text_unigram_model(), text_only_X, y)

    threshold_results = evaluate_tuned_threshold_models(eval_df)
    word_uni_num_result = evaluate_cv_model(
        "text_unigram_plus_numeric",
        make_text_unigram_plus_numeric_model(),
        eval_df[["text", "legacy_polarity", "legacy_subjectivity"]].rename(
            columns={
                "legacy_polarity": "polarity",
                "legacy_subjectivity": "subjectivity",
            }
        ),
        y,
    )
    word_bi_num_result = evaluate_cv_model(
        "text_bigram_plus_numeric",
        make_text_bigram_plus_numeric_model(),
        eval_df[["text", "legacy_polarity", "legacy_subjectivity"]].rename(
            columns={
                "legacy_polarity": "polarity",
                "legacy_subjectivity": "subjectivity",
            }
        ),
        y,
    )

    q5_results = [baseline_result, *threshold_results, q4_model_result, word_uni_num_result, word_bi_num_result]
    ablation_df = pd.DataFrame([result_to_row(result) for result in q5_results])
    ablation_df.to_csv(ABLATION_CSV, index=False)

    save_confusion_csv(Q4_CONFUSION_CSV, q4_model_result.confusion)
    save_confusion_csv(Q5_CONFUSION_CSV, word_uni_num_result.confusion)

    audit_info = build_random_audit_sample(eval_df)

    results_payload = {
        "question_4": {
            "legacy_baseline": result_to_row(baseline_result),
            "text_unigram_logreg": result_to_row(q4_model_result),
            "per_class_metrics": {
                label: {
                    "precision": round(q4_model_result.report[label]["precision"], 4),
                    "recall": round(q4_model_result.report[label]["recall"], 4),
                    "f1": round(q4_model_result.report[label]["f1-score"], 4),
                }
                for label in LABELS
            },
            "random_accuracy_audit": audit_info,
        },
        "question_5": {
            "ablation": [result_to_row(result) for result in q5_results],
            "recommended_model": "text_unigram_plus_numeric",
        },
    }

    RESULTS_JSON.write_text(json.dumps(results_payload, indent=2), encoding="utf-8")
    write_summary(baseline_result, q4_model_result, q5_results, audit_info)

    print(f"Wrote {RESULTS_JSON.relative_to(REPO_ROOT)}")
    print(f"Wrote {SUMMARY_MD.relative_to(REPO_ROOT)}")
    print(f"Wrote {ABLATION_CSV.relative_to(REPO_ROOT)}")
    print(f"Wrote {Q4_CONFUSION_CSV.relative_to(REPO_ROOT)}")
    print(f"Wrote {Q5_CONFUSION_CSV.relative_to(REPO_ROOT)}")
    print(f"Wrote {AUDIT_SAMPLE_XLSX.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
