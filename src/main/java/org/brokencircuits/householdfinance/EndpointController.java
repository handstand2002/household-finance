package org.brokencircuits.householdfinance;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.brokencircuits.householdfinance.domain.CalendarEvent;
import org.brokencircuits.householdfinance.domain.Transaction;
import org.brokencircuits.householdfinance.persistence.InventoryRepository;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Slf4j
@Controller
@RequiredArgsConstructor
public class EndpointController {

  private final InventoryRepository txRepo;
  private static final Pattern amountPattern = Pattern.compile("\\$?(-?)([0-9]+)(\\.([0-9]{2}))?");

  @GetMapping(value = "/update")
  public String doSomething() {
    txRepo.save(new Transaction("tx-1", new BigDecimal(120), "BANNER", "BANNER", Instant.now()));
    return "update";
  }

  @RequestMapping(value = "/addTransaction", method = RequestMethod.POST)
  public String addTransaction(Transaction tx, Model model) {

    log.info("Adding transaction: {}", tx);
    txRepo.save(tx);

    return "redirect:/transactions";
  }

  @RequestMapping(value = "/addTransaction", method = RequestMethod.GET)
  public String addTransaction(Model model) {

    return "addTransaction";
  }

  private List<CalendarEvent> getDateBalances() {

    List<Transaction> transactions = StreamSupport.stream(txRepo.findAll().spliterator(), false)
        .sorted(Comparator.comparingLong(tx -> tx.getTimestamp().toEpochMilli()))
        .collect(Collectors.toList());

    LocalDate crawler = LocalDate.now().minusDays(5);
    LocalDate endDate = LocalDate.now().plusDays(5);
    long currentCents = 0;
    int txIndex = 0;

    List<CalendarEvent> events = new LinkedList<>();
    while (crawler.isBefore(endDate)) {

      long sodCents = currentCents;
      long lowestPoint = currentCents;
      Instant zonedDateTime = crawler.plusDays(1).atStartOfDay()
          .atZone(ZoneId.systemDefault()).toInstant();

      Transaction tx;
      while (transactions.size() > txIndex &&
          (tx = transactions.get(txIndex)).getTimestamp().isBefore(zonedDateTime)) {
        long cents = tx.getAmount().multiply(new BigDecimal(100)).longValue();
        currentCents += cents;

        if (cents < 0) {
          lowestPoint += cents;
        }
        txIndex++;
      }

      long eodCents = currentCents;

      if (sodCents != eodCents) {
        events.add(new CalendarEvent(crawler, String.format("  SOD: $%.2f", sodCents / 100D)));
      }
      if (lowestPoint != sodCents && lowestPoint != eodCents) {
        events.add(new CalendarEvent(crawler, String.format(" Lowest: $%.2f", lowestPoint / 100D)));
      }

      events.add(new CalendarEvent(crawler, String.format("EOD: $%.2f", eodCents / 100D)));

      crawler = crawler.plusDays(1);
    }

    return events;
  }

  @RequestMapping(value = "/transactions", method = RequestMethod.GET)
  public void transactionList(Model model) {
    List<Transaction> outputList = StreamSupport.stream(txRepo.findAll().spliterator(), false)
        .collect(Collectors.toList());

    log.info("Found {} transactions", outputList.size());

    model.addAttribute("transactions", outputList);

    List<CalendarEvent> dateBalances = getDateBalances();
    model.addAttribute("dates", dateBalances);
  }

}
