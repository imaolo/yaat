import itertools, time
from pprint import pprint
from yaat.main import maester, train, parse_args
from tqdm import tqdm



# parameters to combination
# label len?
# probsparse attention factor
# padding?

if False:

    features = ['SPY', 'QQQ', 'XLF', 'XLK', 'VXX']
    fields =  ['close', 'high', 'low']

    # Generate all subsets of features
    feature_combos = []
    for r in range(1, len(features) + 1):
        feature_combos.extend(list(itertools.combinations(features, r)))
    pprint(feature_combos)

    # Generate all subsets of fields
    field_combos = []
    for r in range(1, len(fields) + 1):
        field_combos.extend(list(itertools.combinations(fields, r)))
    pprint(field_combos)

    # Generate all combinations for the single value variables
    arguments_values = {
        'targets': ['SPY', 'QQQ', 'XLF', 'XLK', 'VXX'],
        'attn': ['prob', 'full'],
        'output_attention': [True, False],
        'mix': [True, False],
        'use_amp': [True, False],
        'distill': [True, False],
    }
    single_arg_combos = [dict(zip(arguments_values.keys(), combo)) for combo in list(itertools.product(*arguments_values.values()))]
    pprint(single_arg_combos)

    total_combos = len(feature_combos) * len(field_combos) * len(single_arg_combos)
    print("total: ", len(feature_combos) * len(field_combos) * len(single_arg_combos))

# just simple shit
arguments_values = {
    'attn': ['prob', 'full'],
    'output_attention': [True, False],
    'mix': [True, False],
    'distil': [True, False],
}
single_arg_combos = [dict(zip(arguments_values.keys(), combo)) for combo in list(itertools.product(*arguments_values.values()))]
pprint(single_arg_combos)
print(len(single_arg_combos))


unique_name = str(time.time())
print(unique_name)
for idx, combo in enumerate(tqdm(single_arg_combos)):
    print(combo)
    train_args = {'name': f"{unique_name}_{idx}",
                'tickers': 'SPY',
                'target': 'SPY_open',
                'fields': 'open',
                'seq_len': '128',
                'pred_len': '32',
                'train_epochs': '1',
                'start_date': '2024-7-1',
                'attn': combo['attn'],
                'sample_scale': None
                }
    combo.pop('attn')
    for k,v in combo.items():
        if v: train_args.update({k: None})
    train_args = parse_args('train', train_args)
    train(train_args)

query = {"name": {"$regex": str(unique_name), "$options": "i"}}
pprint(list(maester.informers.find(query, {field:1 for field in arguments_values.keys()} | {'test_loss':1, '_id':0}).sort('test_loss', 1)))
