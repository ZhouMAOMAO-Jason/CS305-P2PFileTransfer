import hashlib
import pickle


def check_target_result(target_file, result_file):
    target_hash = []
    with open(target_file, "r") as tf:
        while True:
            line = tf.readline()
            if not line:
                break
            index, hash_str = line.split(" ")
            hash_str = hash_str.strip()
            target_hash.append(hash_str)
    print('target_list',target_hash)

    with open(result_file, "rb") as rf:
        result_fragments = pickle.load(rf)
        # print('download_keys',result_fragments.keys())
        print('ours',result_fragments.keys())

    for th in target_hash:
        assert th in result_fragments, f"download hash mismatch for target {target_file}, target: {th}, has: {result_fragments.keys()}"

        sha1 = hashlib.sha1()
        sha1.update(result_fragments[th])
        received_hash_str = sha1.hexdigest()
        print('recovery',received_hash_str)
        try:
            assert th.strip() == received_hash_str.strip(), f"received data mismatch for target {target_file}, expect hash: {target_hash}, actual: {received_hash_str}"
        except:
            print('LD')


# check_target_result("tmp5/targets/target1.chunkhash", "tmp5/results/result1.fragment")
# check_target_result("tmp5/targets/target2.chunkhash", "tmp5/results/result2.fragment")
# check_target_result("tmp5/targets/target3.chunkhash", "tmp5/results/result3.fragment")
# check_target_result("tmp5/targets/target4.chunkhash", "tmp5/results/result4.fragment")