"""

  funnels/funnel.py - Fast in-memory user funnel measurement
  Copyright (C) 2020 TJ Murphy <teej.murphy@gmail.com>

  Run from the command line with:

  python3 funnel.py --data my_user_data.csv \
                    --start_event event_a \
                    --end_event event_b \
                    --gap 60
"""

import csv
import sys
import time

from argparse import ArgumentParser

def main(data, start_event, end_event, gap):
  """
  General procedure
    1. Accept a funnel (A then B within 120 sec)
    2. For each user, walk through the event arrays for A & B, count valid matches
    3. Report count of users, average time from A to B
  
  Args:
      data (str): File name of a csv with user event data. The csv should be formatted:
                   - user_id (int)
                   - event_name (str)
                   - event_timestamp (int, epoch timestamp)
      start_event (str): Funnel start event name
      end_event (str): Funnel end event name
      gap (int): Time in seconds between events
  """
  print('='*80)

  ### Set up memory

  # A set of user IDs
  users = set()

  # A set of event types
  events = set()

  # For each user and event, a list of timestamps when that event was fired
  event_series = {}
  # {
  #   112233: {
  #     'event_a': [111, 113, 118, ...],
  #     'event_b': [112, 120, 138, ...]
  #   }
  # }

  # Read file into memory
  now = time.time()
  with open(data, 'r') as f:
    for (user_id_str, event_name, ts_str) in csv.reader(f):
      user_id, ts = int(user_id_str), int(ts_str)
      users.add(user_id)
      events.add(event_name)
      if user_id not in event_series:
        event_series[user_id] = {}
      if event_name not in event_series[user_id]:
        event_series[user_id][event_name] = []
      event_series[user_id][event_name].append(ts)

  # Convert `users` from a set to a sorted list
  users = sorted(list(users))

  load_time = time.time() - now
  print('-', 'loaded in:', f'{load_time:.2f}s')
  print('-', 'users found:', len(users))
  print('-', 'unique events found:', len(events))

  query(start_event, end_event, users, event_series, gap)

def query(start_event, end_event, users, event_series, gap):
  now = time.time()
  start_count = 0
  end_count = 0
  start_es, end_es = 0, 0
  sequences = 0 #[]
  arrival_total = 0
  hist = [0]*(gap+1)

  for user_id in users:
    try:
      lhs = event_series[user_id][start_event]
      start_count += 1
      rhs = event_series[user_id][end_event]
    except KeyError:
      continue

    start_es += len(lhs)
    end_es += len(rhs)

    lhs_cursor = 0
    rhs_cursor = 0
    found = False

    while lhs_cursor < len(lhs) and rhs_cursor < len(rhs):
      lhs_value = lhs[lhs_cursor]
      rhs_value = rhs[rhs_cursor]

      if lhs_value <= rhs_value:
        if rhs_value - lhs_value <= gap:
          lhs_peek = (lhs[lhs_cursor + 1] if lhs_cursor + 1 < len(lhs) else None)
          if lhs_peek and lhs_peek <= rhs_value and rhs_value - lhs_peek <= gap:
            lhs_cursor += 1
          else:
            sequences += 1
            found = True
            arrival_total += (rhs_value - lhs_value)
            hist[(rhs_value - lhs_value)] += 1
            rhs_cursor += 1
        else:
          lhs_cursor += 1
      else:
        rhs_cursor += 1
    if found:
      end_count += 1

  run_time = time.time() - now
  print('-'*80)
  print('Checking', start_event, '=>', end_event, 'within', gap, 'seconds')
  print('-', 'query ran in', f'{run_time:.2f}s')
  print('-', 'events scanned:', start_es + end_es)
  print('Unique starts:', start_count)
  print('Unique completes:', end_count)
  print('Complete rate:', f'{(end_count * 100.0 / start_count):.2f}%')
  print('Total completes:', sequences)
  print('Completes per user:', f'{(sequences * 1.0 / end_count):.2f}')
  print('Average arrival time:', f'{(arrival_total / sequences):.2f}s')
  print('Approx median arrival time:', f'{median_from_freq_table(hist)}s')

def median_from_freq_table(freq_table):
  total = sum(freq_table)
  midpoint = total / 2
  cume_total, cume_i = 0, 0
  while cume_total < midpoint:
    cume_total += freq_table[cume_i]
    cume_i += 1
  return cume_i

def parse(args):
  parser = ArgumentParser()
  parser.add_argument('--data', required=True)
  parser.add_argument('--start_event', required=True)
  parser.add_argument('--end_event', required=True)
  parser.add_argument('--gap_sec', required=True, type=int)
  args = parser.parse_args(args)

  return (args.data, args.start_event, args.end_event, args.gap_sec)

if __name__ == '__main__':
  data, start_event, end_event, gap_sec = parse(sys.argv[1:])
  main(data, start_event, end_event, gap_sec)
  
