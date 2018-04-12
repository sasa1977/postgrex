defmodule Postgrex.TypeManager do
  @moduledoc false

  use GenServer

  def get(module, key) do
    GenServer.call(__MODULE__, {:get, module, key})
  end

  def start_link() do
    GenServer.start_link(__MODULE__, nil, [name: __MODULE__])
  end

  def init(nil) do
    {:ok, {%{}, %{}}}
  end

  def handle_call({:get, module, key}, {pid, _}, state) do
    {server, state} = get_server(state, module, {module, key}, pid)
    {:reply, server, state}
  end

  def handle_info({:DOWN, mref, _, _, _}, {keys, mons}) do
    {key, mons} = Map.pop(mons, mref)
    state = {Map.delete(keys, key), mons}
    {:noreply, state}
  end

  defp get_server({keys, mons} = state, module, key, caller_pid) do
    case keys do
      %{^key => server} ->
        if Process.alive?(server) do
          {server, state}
        else
          # We can end up here if `:DOWN` message didn't arrive yet.
          # In this case, we'll delete the entry and retry.
          keys = Map.delete(keys, key)
          mons = mons |> Enum.reject(&match?({_mref, ^key}, &1)) |> Map.new()
          get_server({keys, mons}, module, key, caller_pid)
        end
      %{} ->
        {:ok, server} = Postgrex.TypeSupervisor.start_server(module, caller_pid)
        mref = Process.monitor(server)
        state = {Map.put(keys, key, server), Map.put(mons, mref, key)}
        {server, state}
    end
  end
end
