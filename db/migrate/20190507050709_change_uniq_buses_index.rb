class ChangeUniqBusesIndex < ActiveRecord::Migration[5.2]
  disable_ddl_transaction!

  def change
    remove_index :buses, :number
    add_index :buses, [:number, :model], unique: true, algorithm: :concurrently
  end
end
