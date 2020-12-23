simdcsv: src/main.o
	$(CXX) -o $@ $^

.PHONY: clean
clean:
	rm -f src/*.o simdcsv
