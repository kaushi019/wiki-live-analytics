# src/ml_model.py
from river import compose
from river import preprocessing
from river import anomaly

class WikiAnomalyDetector:
    def __init__(self):
        """Initializes an advanced multi-dimensional anomaly detection pipeline."""
        self.model = compose.Pipeline(
            preprocessing.StandardScaler(),  # Updates scales incrementally on the fly
            anomaly.HalfSpaceTrees(
                n_trees=15,
                height=15,
                window_size=300,
                seed=42
            )
        )
        print("🧠 Advanced River Risk Profiler Initialized!")

    def learn_and_score(self, bytes_changed, is_bot, user_edit_count, is_minor):
        """
        Calculates a real-time operational risk score based on content integrity metrics
        and user experience dimensions.
        """
        features = {
            "bytes_changed": float(bytes_changed),
            "is_bot": 1.0 if is_bot else 0.0,
            "user_edit_count": float(user_edit_count),  # 🛡️ Risk Profiling Dimension
            "is_minor": 1.0 if is_minor else 0.0        # 🔍 Content Integrity Dimension
        }
        
        score = self.model.score_one(features)
        self.model.learn_one(features)
        return score
