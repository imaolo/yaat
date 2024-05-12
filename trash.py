from yaat.maester2 import Maester
import torch, io

model = torch.nn.Linear(1, 1)

buffer = io.BytesIO()
torch.save(model.state_dict(), buffer)
buffer.seek(0)

m = Maester(connstr=None)
m.create_dataset('my dataset')
m.create_weights('new model', 'my dataset', buffer.getvalue())
# m.weights_coll.update_one({'name': 'new model'}, {'$set': {'weights': buffer.getvalue()}})




# moddoc = m.weights_coll.find_one({'name': 'new model'})

# model_state_dict = torch.load(io.BytesIO(moddoc['weights']))

# model.load_state_dict(model_state_dict)

# print(model[0].weight.data[0])


# new_document = {
#     'name': 'Model X',
#     'dataset': ObjectId(),  # assuming you have a valid ObjectId
#     'weights': b'\x00\x01',  # some binary data representing weights
#     'train_loss': 0.05,
#     'val_loss': 0.04,
#     'status': 'completed'
# }

# m.weights_coll.insert_one(new_document)