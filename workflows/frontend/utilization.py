from __future__ import absolute_import, division
from workflows.services.common_service import CommonService
import time

class UtilizationStatistics(object):
  '''This class is not thread-safe.'''

  status_history = None

  def __init__(self, summation_period=10):
    self.status_history = []
    self.period = summation_period
    self.update_status( CommonService.SERVICE_STATUS_NEW )

  def update_status(self, new_status):
    timestamp = time.time()
    truncate = -1
    cutoff = timestamp - self.period
    for event in self.status_history[:-1]:
      if event[0] > cutoff:
        truncate = truncate + 1
      else:
        break
    if truncate > 0:
      self.status_history = self.status_history[truncate:]

    self.status_history.append((timestamp, new_status))

  def report(self):
    timestamp = time.time()
    summary = {}
    start_times, statuses = zip(*self.status_history)
    end_times = list(map(lambda x: x[0], self.status_history[1:])) + [ timestamp ]
    durations = map(lambda x: x[0]-x[1], zip(end_times, start_times))
    total_duration = sum(durations)
    for status, duration in zip(statuses, durations):
      summary[status] = summary.get(status, 0) + duration
    summary = { s: d / total_duration for s, d in summary.items() }
    return summary
