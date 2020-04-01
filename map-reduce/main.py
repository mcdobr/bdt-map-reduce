from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()

if rank == 0:
    data = {'abc': 1, 'pi': 3.1415}
    comm.send(data, dest=1, tag=1)
    print('Send data to 1')
elif rank == 1:
    data = comm.recv(source=0, tag=1)
    print('Received data on 0')