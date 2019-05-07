class EntityCounter
  INCREASE_CONDITION_ARRAY = 1
  INCREASE_CONDITION_HASH = 0

  attr_reader :entity_cnt, :arr_cnt

  def initialize
    @arr_cnt = 0
    @hs_cnt = 0
    @entity_cnt = 0
  end

  def a_inc
    @arr_cnt += 1
  end

  def a_dec
    @arr_cnt -= 1
  end

  def h_inc
    @hs_cnt += 1
  end

  def h_dec
    @hs_cnt -= 1

    increase_entity_if_state
  end

  def balanced?
    arr_cnt == INCREASE_CONDITION_ARRAY && hs_cnt == INCREASE_CONDITION_HASH
  end

  private

  attr_reader :hs_cnt

  def increase_entity_if_state
    @entity_cnt += 1 if balanced?
  end
end
