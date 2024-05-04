import queue
import random
import heapq
from heapq import heappush, heappop
import random
from statistics import mean

# Constants based on your input
AVERAGE_ARRIVAL_RATE = 5  # Average time between arrivals in minutes
AVERAGE_SERVICE_RATE = 25  # Average time it takes to service a passenger in minutes
SIMULATION_DURATION = 60 * 8  # Let's assume we run the simulation for 8 hours


#Drivers
#Option 1
def simulate_option_1_with_metrics(AVERAGE_ARRIVAL_RATE, AVERAGE_SERVICE_RATE, SIMULATION_DURATION, NUM_STATIONS=5):
    events = []
    heappush(events, (random.expovariate(1 / AVERAGE_ARRIVAL_RATE), 'arrival', 0, None))  # Initial arrival

    queue = []
    max_queue_length = 0
    waiting_times = []
    customers_served = [0 for _ in range(NUM_STATIONS)]
    next_customer_id = 1
    last_event_time = 0

    while events:
        current_time, event_type, customer_id, station_index = heappop(events)
        last_event_time = max(last_event_time, current_time)

        if event_type == 'arrival':
            queue.append(current_time)
            max_queue_length = max(max_queue_length, len(queue))
            if current_time < SIMULATION_DURATION:
                heappush(events, (
                current_time + random.expovariate(1 / AVERAGE_ARRIVAL_RATE), 'arrival', next_customer_id, None))
                next_customer_id += 1
            # Serve immediately if any station is free
            for i in range(NUM_STATIONS):
                if not any(ev[3] == i and ev[1] == 'departure' for ev in events):
                    service_time = random.expovariate(1 / AVERAGE_SERVICE_RATE)
                    heappush(events, (current_time + service_time, 'departure', customer_id, i))
                    break

        elif event_type == 'departure':
            customers_served[station_index] += 1
            if queue:
                arrival_time = queue.pop(0)
                waiting_time = current_time - arrival_time
                waiting_times.append(waiting_time)
                service_time = random.expovariate(1 / AVERAGE_SERVICE_RATE)
                heappush(events, (current_time + service_time, 'departure', None, station_index))

    rate_of_occupancy = [(count / sum(customers_served) * 100) if sum(customers_served) else 0 for count in
                         customers_served]
    average_waiting_time = mean(waiting_times) if waiting_times else 0
    max_waiting_time = max(waiting_times) if waiting_times else 0

    return {
        "total_simulation_time": last_event_time,
        "max_queue_length": max_queue_length,
        "average_waiting_time": average_waiting_time,
        "max_waiting_time": max_waiting_time,
        "customers_served": sum(customers_served),
        "rate_of_occupancy": rate_of_occupancy
    }


#Option 2A Drivers
def simulate_option_2A_complete(AVERAGE_ARRIVAL_RATE, AVERAGE_SERVICE_RATE, SIMULATION_DURATION, NUM_STATIONS=5):
    events = []
    heapq.heappush(events, (random.expovariate(1 / AVERAGE_ARRIVAL_RATE), 'arrival', 0, 0))  # Start with station 0

    queues = [[] for _ in range(NUM_STATIONS)]
    max_queue_lengths = [0 for _ in range(NUM_STATIONS)]
    waiting_times = [[] for _ in range(NUM_STATIONS)]
    customers_served = [0 for _ in range(NUM_STATIONS)]
    next_customer_id = 1
    next_station_index = 1  # For round-robin assignment
    last_event_time = 0  # Track the last event time for total simulation time calculation

    while events:
        current_time, event_type, customer_id, station_index = heapq.heappop(events)
        last_event_time = max(last_event_time, current_time)  # Update last event time

        if event_type == 'arrival':
            arrival_time = current_time
            queues[station_index].append(arrival_time)
            max_queue_lengths[station_index] = max(max_queue_lengths[station_index], len(queues[station_index]))
            if current_time < SIMULATION_DURATION:
                heapq.heappush(events, (
                current_time + random.expovariate(1 / AVERAGE_ARRIVAL_RATE), 'arrival', next_customer_id,
                next_station_index))
                next_customer_id += 1
                next_station_index = (next_station_index + 1) % NUM_STATIONS

        elif event_type == 'departure':
            customers_served[station_index] += 1
            service_start_time = max(last_event_time, current_time)
            waiting_time = service_start_time - queues[station_index].pop(0)
            waiting_times[station_index].append(waiting_time)

        # Serve customers in queues if any station is free
        for i in range(NUM_STATIONS):
            if queues[i] and not any(ev[3] == i and ev[1] == 'departure' for ev in events):
                arrival_time = queues[i][0]  # Next customer's arrival time
                service_time = random.expovariate(1 / AVERAGE_SERVICE_RATE)
                heapq.heappush(events, (current_time + service_time, 'departure', None, i))

    rate_of_occupancy = [(count / sum(customers_served) * 100) if sum(customers_served) else 0 for count in
                         customers_served]
    average_waiting_times = [mean(times) if times else 0 for times in waiting_times]
    max_waiting_times = [max(times) if times else 0 for times in waiting_times]

    return {
        "total_simulation_time": last_event_time,
        "max_queue_lengths": max_queue_lengths,
        "average_waiting_times": average_waiting_times,
        "max_waiting_times": max_waiting_times,
        "customers_served": customers_served,
        "rate_of_occupancy": rate_of_occupancy
    }


#Option 2B Drivers
def simulate_option_2B(AVERAGE_ARRIVAL_RATE, AVERAGE_SERVICE_RATE, SIMULATION_DURATION, NUM_STATIONS=5):
    events = []
    heappush(events, (random.expovariate(1 / AVERAGE_ARRIVAL_RATE), 'arrival', 0, None))  # Initial arrival

    queues = [[] for _ in range(NUM_STATIONS)]
    max_queue_lengths = [0 for _ in range(NUM_STATIONS)]
    waiting_times = [[] for _ in range(NUM_STATIONS)]
    customers_served = [0 for _ in range(NUM_STATIONS)]
    next_customer_id = 1
    last_event_time = 0

    while events:
        current_time, event_type, customer_id, station_index = heappop(events)
        last_event_time = max(last_event_time, current_time)

        if event_type == 'arrival':
            if current_time < SIMULATION_DURATION:
                heappush(events, (
                current_time + random.expovariate(1 / AVERAGE_ARRIVAL_RATE), 'arrival', next_customer_id, None))
                next_customer_id += 1
            # Find the shortest queue
            shortest_queue_index = min(range(NUM_STATIONS), key=lambda i: len(queues[i]))
            queues[shortest_queue_index].append(current_time)
            max_queue_lengths[shortest_queue_index] = max(max_queue_lengths[shortest_queue_index],
                                                          len(queues[shortest_queue_index]))
            # Immediately serve if station is free
            if not any(ev[3] == shortest_queue_index and ev[1] == 'departure' for ev in events):
                service_time = random.expovariate(1 / AVERAGE_SERVICE_RATE)
                heappush(events, (current_time + service_time, 'departure', customer_id, shortest_queue_index))

        elif event_type == 'departure':
            customers_served[station_index] += 1
            arrival_time = queues[station_index].pop(0)
            waiting_time = current_time - arrival_time
            waiting_times[station_index].append(waiting_time)
            # Check if more customers are waiting and serve next
            if queues[station_index]:
                next_service_time = random.expovariate(1 / AVERAGE_SERVICE_RATE)
                heappush(events, (current_time + next_service_time, 'departure', None, station_index))

    rate_of_occupancy = [(count / sum(customers_served) * 100) if sum(customers_served) else 0 for count in
                         customers_served]
    average_waiting_times = [mean(times) if times else 0 for times in waiting_times]
    max_waiting_times = [max(times or [0]) for times in waiting_times]

    return {
        "total_simulation_time": last_event_time,
        "max_queue_lengths": max_queue_lengths,
        "average_waiting_times": average_waiting_times,
        "max_waiting_times": max_waiting_times,
        "customers_served": customers_served,
        "rate_of_occupancy": rate_of_occupancy
    }


def simulate_option_2C(AVERAGE_ARRIVAL_RATE, AVERAGE_SERVICE_RATE, SIMULATION_DURATION, NUM_STATIONS=5):
    events = []
    heappush(events, (random.expovariate(1 / AVERAGE_ARRIVAL_RATE), 'arrival', 0, None))  # Initial arrival

    queues = [[] for _ in range(NUM_STATIONS)]
    max_queue_lengths = [0 for _ in range(NUM_STATIONS)]
    waiting_times = [[] for _ in range(NUM_STATIONS)]
    customers_served = [0 for _ in range(NUM_STATIONS)]
    next_customer_id = 1
    last_event_time = 0

    while events:
        current_time, event_type, customer_id, station_index = heappop(events)
        last_event_time = max(last_event_time, current_time)

        if event_type == 'arrival':
            if current_time < SIMULATION_DURATION:
                heappush(events, (
                current_time + random.expovariate(1 / AVERAGE_ARRIVAL_RATE), 'arrival', next_customer_id, None))
                next_customer_id += 1
            # Assign customer to a random queue
            random_queue_index = random.randrange(NUM_STATIONS)
            queues[random_queue_index].append(current_time)
            max_queue_lengths[random_queue_index] = max(max_queue_lengths[random_queue_index],
                                                        len(queues[random_queue_index]))
            # Serve immediately if the station is free
            if not any(ev[3] == random_queue_index and ev[1] == 'departure' for ev in events):
                service_time = random.expovariate(1 / AVERAGE_SERVICE_RATE)
                heappush(events, (current_time + service_time, 'departure', customer_id, random_queue_index))

        elif event_type == 'departure':
            customers_served[station_index] += 1
            arrival_time = queues[station_index].pop(0)
            waiting_time = current_time - arrival_time
            waiting_times[station_index].append(waiting_time)
            # Serve next customer if queue is not empty
            if queues[station_index]:
                next_service_time = random.expovariate(1 / AVERAGE_SERVICE_RATE)
                heappush(events, (current_time + next_service_time, 'departure', None, station_index))

    rate_of_occupancy = [(count / sum(customers_served) * 100) if sum(customers_served) else 0 for count in
                         customers_served]
    average_waiting_times = [mean(times) if times else 0 for times in waiting_times]
    max_waiting_times = [max(times or [0]) for times in waiting_times]

    return {
        "total_simulation_time": last_event_time,
        "max_queue_lengths": max_queue_lengths,
        "average_waiting_times": average_waiting_times,
        "max_waiting_times": max_waiting_times,
        "customers_served": customers_served,
        "rate_of_occupancy": rate_of_occupancy
    }


results_option_1_with_metrics = simulate_option_1_with_metrics(AVERAGE_ARRIVAL_RATE, AVERAGE_SERVICE_RATE, SIMULATION_DURATION)
print(results_option_1_with_metrics)
results_option_2A_complete = simulate_option_2A_complete(AVERAGE_ARRIVAL_RATE, AVERAGE_SERVICE_RATE, SIMULATION_DURATION)
print(results_option_2A_complete)
results_option_2B = simulate_option_2B(AVERAGE_ARRIVAL_RATE, AVERAGE_SERVICE_RATE, SIMULATION_DURATION)
print(results_option_2B)
results_option_2C = simulate_option_2C(AVERAGE_ARRIVAL_RATE, AVERAGE_SERVICE_RATE, SIMULATION_DURATION)
print(results_option_2C)

