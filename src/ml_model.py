# src/ml_model.py
from river import compose
from river import preprocessing
from river import anomaly

class WikiAnomalyDetector:
    def __init__(self):
        """Initializes an incremental, streaming anomaly detection pipeline."""
        # HalfSpaceTrees is perfect for multi-dimensional streaming anomaly detection
        self.model = compose.Pipeline(
            preprocessing.StandardScaler(),  # Scales features incrementally on the fly
            anomaly.HalfSpaceTrees(
                n_trees=10,
                height=15,
                window_size=250,             # Learns from a moving window of recent edits
                seed=42
            )
        )
        print("🧠 River Streaming Anomaly Detector initialized successfully!")

    def learn_and_score(self, bytes_changed, is_bot):
        """
        Learns from a single data point and returns its anomaly score.
        Score ranges from 0.0 (perfectly normal) to 1.0 (highly anomalous).
        """
        # Prepare the feature dictionary for River
        features = {
            "bytes_changed": float(bytes_changed),
            "is_bot": 1.0 if is_bot else 0.0
        }
        
        # 1. Get the anomaly score *before* updating the model 
        # (gives an unbiased score based on what it currently knows)
        score = self.model.score_one(features)
        
        # 2. Update the model's internal weights with the new observation
        self.model.learn_one(features)
        
        return score
