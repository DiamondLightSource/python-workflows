from __future__ import absolute_import, division
import mock
import workflows.frontend.utilization
from workflows.services.common_service import CommonService

def about(value, tolerance):
  class comparator():
    def __le__(self, other):
      return other >= value-tolerance
    def __eq__(self, other):
      return other >= value-tolerance and other <= value+tolerance
    def __ge__(self, other):
      return other <= value+tolerance
    def __ne__(self, other):
      return not self.__eq__(other)
    def __repr__(self):
      return "<%s +- %s>" % (str(value), str(tolerance))
  return comparator()

def test_near_equality_helper_class():
  three = about(3, 0.11)
  assert 2   <= three
  assert 2   != three
  assert 2.8 != three
  assert 2.9 == three
  assert 3.0 == three
  assert 3   == three
  assert 3.1 == three
  assert 3.2 != three
  assert 4   != three
  assert 4   >= three

def test_get_empty_statistics_report():
  stat = workflows.frontend.utilization.UtilizationStatistics(summation_period=10)
  assert stat.report() == { CommonService.SERVICE_STATUS_NEW: 1.0 }

@mock.patch('workflows.frontend.utilization.time')
def test_statistics_report_contains_correctly_aggregated_information(t):
  t.time.return_value = 100000
  stat = workflows.frontend.utilization.UtilizationStatistics(summation_period=10)
  t.time.return_value = 100005
  assert stat.report() == { CommonService.SERVICE_STATUS_NEW: 1.0 }
  stat.update_status( CommonService.SERVICE_STATUS_IDLE )
  t.time.return_value = 100010
  assert stat.report() == { CommonService.SERVICE_STATUS_NEW: about(0.5, 0.01), CommonService.SERVICE_STATUS_IDLE: about(0.5, 0.01) }
  stat.update_status( 127 )
  t.time.return_value = 100012
  assert stat.report() == { CommonService.SERVICE_STATUS_NEW: about(0.3, 0.01), CommonService.SERVICE_STATUS_IDLE: about(0.5, 0.01), 127: about(0.2, 0.01) }
  stat.update_status( CommonService.SERVICE_STATUS_IDLE )
  t.time.return_value = 100013
  assert stat.report() == { CommonService.SERVICE_STATUS_NEW: about(0.2, 0.01), CommonService.SERVICE_STATUS_IDLE: about(0.6, 0.01), 127: about(0.2, 0.01) }
  stat.update_status( 128 )
  t.time.return_value = 100016
  assert stat.report() == { CommonService.SERVICE_STATUS_IDLE: about(0.5, 0.01), 127: about(0.2, 0.01), 128: about(0.3, 0.01) }
  t.time.return_value = 100017
  assert stat.report() == { CommonService.SERVICE_STATUS_IDLE: about(0.4, 0.01), 127: about(0.2, 0.01), 128: about(0.4, 0.01) }
  t.time.return_value = 100018
  assert stat.report() == { CommonService.SERVICE_STATUS_IDLE: about(0.3, 0.01), 127: about(0.2, 0.01), 128: about(0.5, 0.01) }
  t.time.return_value = 100019
  assert stat.report() == { CommonService.SERVICE_STATUS_IDLE: about(0.2, 0.01), 127: about(0.2, 0.01), 128: about(0.6, 0.01) }
  t.time.return_value = 100020
  assert stat.report() == { CommonService.SERVICE_STATUS_IDLE: about(0.1, 0.01), 127: about(0.2, 0.01), 128: about(0.7, 0.01) }
  t.time.return_value = 100021
  assert stat.report() == { CommonService.SERVICE_STATUS_IDLE: about(0.1, 0.01), 127: about(0.1, 0.01), 128: about(0.8, 0.01) }
  t.time.return_value = 100022
  assert stat.report() == { CommonService.SERVICE_STATUS_IDLE: about(0.1, 0.01), 127: about(0.0, 0.01), 128: about(0.9, 0.01) }
  t.time.return_value = 100022.001
  assert stat.report() == { CommonService.SERVICE_STATUS_IDLE: about(0.1, 0.01), 128: about(0.9, 0.01) }
  t.time.return_value = 100023
  assert stat.report() == { CommonService.SERVICE_STATUS_IDLE: about(0.0, 0.01), 128: about(1.0, 0.01) }
  t.time.return_value = 100023.001
  assert stat.report() == { 128: 1.0 }
