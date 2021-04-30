import csv
import os
import sys


def build_third_label_pool(csv_name, path_to_csv='/opt/datacenter/audioset/'):
    csv_file = os.path.join(path_to_csv, csv_name)
    with open(csv_file, 'r') as f:
        csv_data = csv.reader(f)

        label_list = []
        third_to_first_dict  = {}
        third_to_second_dict = {}
        try:
            for row_idx, row in enumerate(csv_data):
                if row_idx == 0:
                    continue
                labels = row[0]
                label_list.append(labels)
                #print(labels)
                if row[2] != '':
                    third_to_first_dict[labels] = row[2]
                if row[3] != '':
                    third_to_second_dict[labels] = row[3]

        except csv.Error as e:
            err_msg = 'Encountered error in {} at line {}: {}'
            LOGGER.error(err_msg)
            sys.exit(err_msg.format(subset_path, row_idx + 1, e))

        except KeyboardInterrupt:
            LOGGER.info("Forcing exit.")
            exit()

    return label_list, third_to_first_dict, third_to_second_dict

def build_fourth_label_pool(csv_name, path_to_csv='/opt/datacenter/audioset/'):
    csv_file = os.path.join(path_to_csv, csv_name)
    with open(csv_file, 'r') as f:
        csv_data = csv.reader(f)

        label_list = []
        dict_labels = {}
        try:
            for row_idx, row in enumerate(csv_data):
                if row_idx == 0:
                    continue

                third_label = row[0]
                fourth_labels = row[4:]
                for i in range(len(fourth_labels)):
                    if fourth_labels[i] != '':
                        #print(labels[i])
                        label_list.append(fourth_labels[i])
                        dict_labels[fourth_labels[i]] = third_label

        except csv.Error as e:
            err_msg = 'Encountered error in {} at line {}: {}'
            LOGGER.error(err_msg)
            sys.exit(err_msg.format(subset_path, row_idx + 1, e))

        except KeyboardInterrupt:
            LOGGER.info("Forcing exit.")
            exit()

    return label_list, dict_labels


def label_filter(csv_name, path_to_csv, fourth_to_third_dict, third_to_first_dict, third_to_second_dict):
    # step 0: read file
    csv_file = os.path.join(path_to_csv, csv_name)
    filtered_csv_file = os.path.join(path_to_csv, csv_name[:-4]+'_filtered.csv')
    with open(csv_file, 'r') as f:
        csv_data = csv.reader(f)

        with open(filtered_csv_file, 'w') as g:
            writer = csv.writer(g)

            try:
                for row_idx, row in enumerate(csv_data):
                # Skip commented lines
                    if row[0][0] == '#' or row[0][0] == '/':
                        writer.writerow(row)
                        continue

                    current_labels = row[3:]
                    current_labels[0] = current_labels[0][2:]
                    current_labels[-1] = current_labels[-1][:-1]
                    # step 1: removing the fourth
                    current_labels_v1 = []
                    for i in range(len(current_labels)):
                        label = current_labels[i]
                        try:
                            third_label = fourth_to_third_dict[label]
                            current_labels_v1.append(third_label)
                        except:
                            current_labels_v1.append(label)

                    current_labels_v1 = list(set(current_labels_v1))

                    # step 2: generate the first and second pool
                    current_first_pool  = []
                    current_second_pool = []
                    for i in range(len(current_labels_v1)):
                        label = current_labels_v1[i]
                        try:
                            first_label = third_to_first_dict[label]
                            current_first_pool.append(first_label)
                        except:
                            pass

                        try:
                            second_label = third_to_second_dict[label]
                            current_second_pool.append(second_label)
                        except:
                            pass

                    # step 3: removing the repeated labels
                    current_labels_v2 = []
                    for i in range(len(current_labels_v1)):
                        label = current_labels_v1[i]
                        if (label not in current_first_pool) and (label not in current_second_pool):
                            current_labels_v2.append(label)

                    del(row[3:])
                    row.extend(current_labels_v2)
                    writer.writerow(row)

            except csv.Error as e:
                err_msg = 'Encountered error in {} at line {}: {}'
                LOGGER.error(err_msg)
                sys.exit(err_msg.format(subset_path, row_idx + 1, e))

            except KeyboardInterrupt:
                LOGGER.info("Forcing exit.")
                exit()



if __name__ == '__main__':

    path_to_file = '/opt/datacenter/audioset/'
    csv_file = 'unbalanced_train_segments.csv'
    label_topology = 'class_topology.csv'

    # preparing
    third_label_pool, third_to_first_dict, third_to_second_dict = build_third_label_pool(label_topology, path_to_csv='')
    fourth_label_pool, fourth_to_third_dict = build_fourth_label_pool(label_topology, path_to_csv='')

    label_filter(csv_file, path_to_file, fourth_to_third_dict, third_to_first_dict, third_to_second_dict)

