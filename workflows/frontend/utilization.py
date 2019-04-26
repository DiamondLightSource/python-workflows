from __future__ import absolute_import, division, print_function

import time

from workflows.services.common_service import CommonService


class UtilizationStatistics(object):
    """Generate statistics about the percentage of time spent in different
    statuses over a fixed time slice. This class is not thread-safe."""

    def __init__(self, summation_period=10):
        """Reports will always cover the most recent period of summation_period
        seconds."""
        self.period = summation_period
        self.status_history = [
            {"start": 0, "end": None, "status": CommonService.SERVICE_STATUS_NEW}
        ]

    def update_status(self, new_status):
        """Record a status change with a current timestamp."""
        timestamp = time.time()
        self.status_history[-1]["end"] = timestamp
        self.status_history.append(
            {"start": timestamp, "end": None, "status": new_status}
        )

    def report(self):
        """Return a dictionary of different status codes and the percentage of time
        spent in each throughout the last summation_period seconds.
        Truncate the aggregated history appropriately."""
        timestamp = time.time()
        cutoff = timestamp - self.period
        truncate = 0
        summary = {}
        for event in self.status_history[:-1]:
            if event["end"] < cutoff:
                truncate = truncate + 1
                continue
            summary[event["status"]] = (
                summary.get(event["status"], 0)
                + event["end"]
                - max(cutoff, event["start"])
            )
        summary[self.status_history[-1]["status"]] = (
            summary.get(self.status_history[-1]["status"], 0)
            + timestamp
            - max(cutoff, self.status_history[-1]["start"])
        )
        if truncate:
            self.status_history = self.status_history[truncate:]
        total_duration = sum(summary.values())
        summary = {s: round(d / total_duration, 4) for s, d in summary.items()}
        return summary
