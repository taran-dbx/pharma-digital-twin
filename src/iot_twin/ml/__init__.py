"""Spindle 3 thermal-risk model: shared features, labels and the alert contract.

Pure Python (no numpy/pandas) so the same code runs in the app process, which
scores live readings, and in the serverless training job, which builds the
training set. Training and serving import the same functions, so they cannot drift.
"""
