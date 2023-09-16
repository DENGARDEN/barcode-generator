import random
from icecream import ic
from tqdm import tqdm
import multiprocessing
from joblib import Parallel, delayed
import pandas as pd
from datetime import datetime

NUM_CORES = multiprocessing.cpu_count()


# Function to generate a random nucleotide sequence of length 'l'
def generate_sequence(l):
    nucleotides = ["A", "C", "G", "T"]
    sequence = "".join(random.choice(nucleotides) for _ in range(l))
    return sequence


# Function to check if a sequence contains repetitive nucleotides (Constraint 1)
def contains_repetitive(sequence):
    for i in range(len(sequence) - 1):
        if sequence[i] == sequence[i + 1]:
            return True
    return False


# Function to check if a sequence differs by at least two bases from existing sequences (Constraint 2)
def differs_by_at_least_two(sequence, existing_sequences):
    for existing_seq in existing_sequences:
        diff_count = sum(1 for a, b in zip(sequence, existing_seq) if a != b)
        if diff_count < 2:
            return False
    return True


# Function to generate a unique sequence of length 'l' that meets both constraints
def generate_unique_sequence(l, existing_sequences):
    while True:
        sequence = generate_sequence(l)
        if not contains_repetitive(sequence) and differs_by_at_least_two(
            sequence, existing_sequences
        ):
            return sequence


def barcode_generator(num_sequences, sequence_length):
    ic(num_sequences, sequence_length)

    def generate_sequence_wrapper(storing_object):
        unique_sequence = generate_unique_sequence(sequence_length, storing_object)

        return unique_sequence

    def call_parallel_jobs(n, storing_object):
        with Parallel(n_jobs=NUM_CORES) as parallel:
            generation_results = parallel(
                delayed(generate_sequence_wrapper)(storing_object)
                for _ in tqdm(range(int(n)))
            )
            return generation_results

    first_shot_sequences = []
    first_shot_sequences = call_parallel_jobs(num_sequences, first_shot_sequences)
    first_shot_sequences = list(set(first_shot_sequences))
    ic("First shot sequences generated", len(set(first_shot_sequences)))
    ic(
        f"Remaining {num_sequences - len(set(first_shot_sequences))} will be generated with stricter manner"
    )

    # Second shot
    manager = multiprocessing.Manager()
    second_shot_sequences = manager.list()
    second_shot_sequences.extend(
        first_shot_sequences
    )  # Use a shared list to ensure uniqueness

    second_shot_sequences.extend(
        call_parallel_jobs(
            num_sequences - len(set(first_shot_sequences)), second_shot_sequences
        )
    )

    assert (
        len(set(second_shot_sequences)) == num_sequences
    ), "The number of sequences generated is not equal to the number of sequences requested"

    return list(second_shot_sequences)  # Convert the shared list to a regular list


if __name__ == "__main__":
    # Number of sequences you want to generate
    # TODO : possibility calculation
    NUM_SEQUENCES = 22971
    SEQUENCE_LENGTH = 11

    ic(NUM_CORES)
    sequences = barcode_generator(NUM_SEQUENCES, SEQUENCE_LENGTH)

    ic(len(sequences))
    # ic(sequences)

    # Now 'sequences' contains a unique set of barcodes
    ic(pd.Series(sequences).is_unique)
    if pd.Series(sequences).is_unique:
        pd.Series(sequences).to_csv(
            f"./Barcodes/n={int(NUM_SEQUENCES)}-{SEQUENCE_LENGTH}nt-{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.csv",
            index=False,
            header=False,
        )
